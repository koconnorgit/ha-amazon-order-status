"""Amazon Orders Data Coordinator."""

from __future__ import annotations

import imaplib
import logging
import re
from datetime import datetime, timedelta, timezone
import email.utils
from email import message_from_bytes
from email.header import decode_header
from typing import Dict

from homeassistant.core import HomeAssistant, callback
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.helpers.storage import Store
import html
from bs4 import BeautifulSoup

from .const import CONF_MARK_AS_READ, CONF_IMAP_FOLDER

_LOGGER = logging.getLogger(__name__)


def _parse_internaldate(internaldate_str: str) -> datetime | None:
    """Parse IMAP INTERNALDATE string to timezone-aware UTC datetime."""
    try:
        # Format: "08-Feb-2025 18:30:00 +0000" or "08-Feb-2025 10:30:00 -0800"
        internaldate_str = internaldate_str.strip()
        if len(internaldate_str) < 26:
            return None
        dt = datetime.strptime(internaldate_str[:20], "%d-%b-%Y %H:%M:%S")
        tz_str = internaldate_str[20:].strip()
        if not tz_str:
            return dt.replace(tzinfo=timezone.utc)
        sign = -1 if tz_str[0] == "-" else 1
        hours = int(tz_str[1:3])
        mins = int(tz_str[3:5]) if len(tz_str) >= 5 else 0
        tz = timezone(timedelta(minutes=sign * (hours * 60 + mins)))
        return dt.replace(tzinfo=tz).astimezone(timezone.utc)
    except (ValueError, IndexError):
        return None


def _to_utc(dt: datetime) -> datetime:
    """Normalize a datetime to UTC for comparison."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
LAST_CHECK_KEY = "last_check"
STORAGE_VERSION = 1
STORAGE_KEY = "amazon_order_status"
ORDERS_KEY = "orders"

ORDER_REGEX = re.compile(
    r"(?:Order|Purchase)\s*(?:#|number|ID|No\.?)?\s*[:#]?\s*"
    r"([0-9]{3}-[0-9]{7}-[0-9]{7}|[0-9\-]{10,})",
    re.IGNORECASE,
)

# IMAP INTERNALDATE format: "08-Feb-2025 18:30:00 +0000"
INTERNALDATE_RE = re.compile(r'INTERNALDATE\s+"([^"]+)"', re.IGNORECASE)

# English month abbreviations for IMAP date (RFC 3501); avoid locale-dependent strftime
_IMAP_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def _imap_date_str(dt: datetime) -> str:
    """Return date in IMAP format dd-Mon-yyyy (English month) for SEARCH SINCE."""
    return f"{dt.day:02d}-{_IMAP_MONTHS[dt.month - 1]}-{dt.year}"

STATUS_MAP = {
    "successfully placed your order": "Ordered",
    "we've received your order": "Ordered",
    "preparing your automatic refill order": "Ordered",
    "automatic refill order": "Ordered",
    "ordered": "Ordered",
    "shipped": "Shipped",
    "out for delivery": "Out for delivery",
    "delivered": "Delivered",
}

def _select_folder(mail: imaplib.IMAP4, folder: str) -> None:
    """Select an IMAP mailbox. Use standard select() when safe; otherwise send SELECT line ourselves.

    For names with space or parentheses, imaplib sends SELECT Test folder (unquoted), which
    FastMail rejects as 'Invalid modifier list'. Sending bytes via _command can be treated
    as a literal. So we build and send the exact line: TAG SELECT "folder"\r\n via mail.send(),
    then wait for the response and set state.
    """
    if any(c in folder for c in " ()"):
        mail.untagged_responses = {}
        quoted = '"' + folder.replace("\\", "\\\\").replace('"', '\\"') + '"'
        tag = mail._new_tag()
        tag_str = tag.decode("ascii") if isinstance(tag, bytes) else tag
        line = tag_str + " SELECT " + quoted + "\r\n"
        mail.send(line.encode("utf-8"))
        typ, data = mail._command_complete("SELECT", tag)
        if typ != "OK":
            msg = data[-1].decode("utf-8", "replace") if data and isinstance(data[-1], bytes) else str(data)
            raise mail.error(
                "Mailbox %r not found: %s. Check the folder name in integration options "
                "(case-sensitive). Use INBOX for the main inbox, or try a path like INBOX.Test folder."
                % (folder, msg)
            )
        mail.state = "SELECTED"
    else:
        mail.select(folder)


class AmazonOrdersCoordinator(DataUpdateCoordinator):
    """Coordinator to fetch and track Amazon orders via email."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry):
        self.hass = hass
        self.entry = entry
        self._store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self._orders: Dict[str, dict] = {}
        self.delivered_retention_days = entry.options.get("delivered_retention_days", 7)
        self._mark_as_read = entry.options.get(CONF_MARK_AS_READ, False)
        # Get IMAP folder from options, default to "INBOX" if empty or not set
        folder = entry.options.get(CONF_IMAP_FOLDER, "")
        self._imap_folder = folder.strip() if folder and folder.strip() else "INBOX"
        self.last_check: datetime | None = None

        # Determine update interval from options or config entry, default 5 min
        interval_minutes = entry.options.get(
            "update_interval", entry.data.get("update_interval", 5)
        )

        super().__init__(
            hass,
            _LOGGER,
            name="Amazon Order Status",
            update_interval=timedelta(minutes=interval_minutes),
        )

    """ Timestamp storage/retrieval functions to reduce email check time """
    async def async_load_last_check(self) -> datetime | None:
        stored = await self._store.async_load()
        if stored and LAST_CHECK_KEY in stored:
            return datetime.fromisoformat(stored[LAST_CHECK_KEY])
        return None

    async def async_load_stored_orders(self) -> None:
        """Load persisted orders from storage."""
        stored = await self._store.async_load()
        if stored:
            self._orders = stored.get(ORDERS_KEY, {})
            _LOGGER.debug("Loaded %d stored Amazon orders", len(self._orders))
        else:
            self._orders = {}
            _LOGGER.debug("No stored Amazon orders found")

    async def async_save_state(self, last_check: datetime) -> None:
        await self._store.async_save(
            {
                LAST_CHECK_KEY: last_check.isoformat(),
                ORDERS_KEY: self._orders,
            }
        )

    async def _async_update_data(self):
        _LOGGER.debug("Coordinator update triggered at %s", datetime.now(timezone.utc))

        if not self._orders:
            await self.async_load_stored_orders()

        last_check = await self.async_load_last_check()
        now = datetime.now(timezone.utc)
        self.last_check = now

        await self.hass.async_add_executor_job(
            self._fetch_and_parse_emails,
            last_check,
            now,
        )

        # Purge old delivered orders
        self._purge_old_delivered_orders(now)

        await self.async_save_state(now)

        # Include order_id in each item so sensors and services can use it
        return [{**v, "order_id": k} for k, v in self._orders.items()]

    @callback
    def async_update_interval(self, minutes: int):
        """Dynamically update the coordinator's refresh interval."""
        self.update_interval = timedelta(minutes=minutes)
        _LOGGER.debug("Coordinator update interval set to %d minutes", minutes)
        self.hass.async_create_task(self.async_refresh())

    @callback
    def async_set_retention_days(self, days: int):
        """Update delivered retention days and immediately purge old delivered orders."""
        self.delivered_retention_days = days
        _LOGGER.debug("Delivered retention days updated to %d", days)
        self._purge_old_delivered_orders(datetime.now(timezone.utc))

    @callback
    def async_set_mark_as_read(self, mark_as_read: bool):
        """Enable or disable marking emails as read."""
        self._mark_as_read = mark_as_read
        _LOGGER.debug("Mark as read option updated: %s", mark_as_read)

        # Update the config entry safely
        new_options = dict(self.entry.options)
        new_options[CONF_MARK_AS_READ] = mark_as_read
        self.hass.config_entries.async_update_entry(self.entry, options=new_options)

    @callback
    def async_set_imap_folder(self, folder: str):
        """Update the IMAP folder to search."""
        folder_clean = folder.strip() if folder and folder.strip() else "INBOX"
        self._imap_folder = folder_clean
        _LOGGER.debug("IMAP folder updated to: %s", self._imap_folder)

        # Update the config entry safely
        new_options = dict(self.entry.options)
        new_options[CONF_IMAP_FOLDER] = folder
        self.hass.config_entries.async_update_entry(self.entry, options=new_options)

    @callback
    def _purge_old_delivered_orders(self, now: datetime):
        """Remove delivered orders older than retention period."""
        if not self._orders:
            return

        retention_cutoff = now - timedelta(days=self.delivered_retention_days)
        to_remove = [
            order_id
            for order_id, order in self._orders.items()
            if order.get("status") == "Delivered"
            and datetime.fromisoformat(order.get("updated")) < retention_cutoff
        ]

        for order_id in to_remove:
            _LOGGER.debug(
                "Purging delivered order %s (older than %d days)",
                order_id,
                self.delivered_retention_days,
            )
            self._orders.pop(order_id, None)

    async def async_purge_order(self, order_id: str) -> bool:
        """Remove a specific order from tracking and persist state. Returns True if removed."""
        if order_id not in self._orders:
            return False
        self._orders.pop(order_id, None)
        _LOGGER.debug("Purged order %s by user request", order_id)
        now = self.last_check or datetime.now(timezone.utc)
        await self.async_save_state(now)
        self.async_set_updated_data(
            [{**v, "order_id": k} for k, v in self._orders.items()]
        )
        return True

    def _fetch_and_parse_emails(self, last_check: datetime | None, now: datetime):
        """Connect to IMAP and parse Amazon emails."""
        email_addr = self.entry.data["email"]
        password = self.entry.data["password"]
        imap_server = self.entry.data["imap_server"]
        mark_as_read = self._mark_as_read

        _LOGGER.debug("Connecting to IMAP server %s as %s", imap_server, email_addr)

        mail = imaplib.IMAP4_SSL(imap_server)
        mail.login(email_addr, password)
        # Send SELECT with mailbox as a quoted string so IMAP4rev2 servers (e.g. FastMail)
        # do not misparse the command as having an invalid modifier list (RFC 9051).
        _select_folder(mail, self._imap_folder)
        _LOGGER.debug("Selected IMAP folder: %s", self._imap_folder)

        if last_check:
            since = last_check
            _LOGGER.debug("Checking emails since last run: %s", since)
        else:
            since = now - timedelta(days=14)
            _LOGGER.debug("First run: checking last 14 days")

        since_utc = _to_utc(since)
        # IMAP SINCE is interpreted in the server's timezone (e.g. Gmail uses PST), so
        # using the UTC date can ask for "future" emails and return 0. Use one day
        # earlier so we always fetch recent emails; we filter by since_utc in Python.
        since_date_imap = _imap_date_str(since_utc - timedelta(days=1))
        # Restrict the search to Amazon emails only. Searching all mail in folders like
        # Gmail All Mail can touch hundreds of unrelated recent messages and block HA
        # startup while every body is fetched.
        # FROM "amazon.com" is broad enough to include the common order/tracking
        # senders (auto-confirm@amazon.com, shipment-tracking@amazon.com, no-reply@amazon.com).
        # No charset for date-only criterion; some servers reject CHARSET with SINCE.
        typ, data = mail.search(None, f'(SINCE "{since_date_imap}" FROM "amazon.com")')

        if typ != "OK":
            _LOGGER.error("IMAP search failed")
            mail.logout()
            return

        email_nums = data[0].split()
        _LOGGER.debug(
            "Found %d emails since %s (processing all; applying updates when email is newer than stored)",
            len(email_nums),
            since_date_imap,
        )

        for num in email_nums:
            typ, msg_data = mail.fetch(num, "(BODY.PEEK[] INTERNALDATE)")
            if typ != "OK" or not msg_data or not msg_data[0]:
                _LOGGER.warning("Failed to fetch email %s", num)
                continue

            # Prefer INTERNALDATE (when mailbox received the message) over Date header
            raw_response = msg_data[0][0]
            if isinstance(raw_response, bytes):
                raw_response = raw_response.decode(errors="ignore")
            internaldate_utc = None
            internaldate_match = INTERNALDATE_RE.search(raw_response)
            if internaldate_match:
                internaldate_utc = _parse_internaldate(internaldate_match.group(1))

            msg = message_from_bytes(msg_data[0][1])
            msg_date = msg.get("Date")
            msg_datetime = None
            if msg_date:
                msg_datetime = email.utils.parsedate_to_datetime(msg_date)
                if msg_datetime.tzinfo is None:
                    msg_datetime = msg_datetime.replace(tzinfo=timezone.utc)
                else:
                    msg_datetime = msg_datetime.astimezone(timezone.utc)

            received_utc = internaldate_utc if internaldate_utc is not None else msg_datetime
            if received_utc is None:
                _LOGGER.debug("Email %s: no INTERNALDATE or Date header, skipping", num)
                continue

            # Ignore emails that arrived before our last check (avoids re-adding purged orders)
            if last_check is not None and received_utc < since_utc:
                _LOGGER.debug(
                    "Email %s: received %s before last check %s, skipping",
                    num,
                    received_utc,
                    since_utc,
                )
                continue

            subject = self._decode_header(msg.get("Subject", ""))
            subject_lower = subject.lower()

            status = self._status_from_subject(subject_lower)
            if not status:
                _LOGGER.debug(
                    "Email %s: subject not recognized as order status, skipping: %s",
                    num,
                    subject[:80],
                )
                continue

            body_text = self._extract_text(msg)
            html_body = self._extract_html(msg)

            # Collect order IDs from both plain text and HTML; many "shipped" emails
            # put the order number only in the HTML part, so we must check both.
            order_ids = list(
                dict.fromkeys(
                    ORDER_REGEX.findall(body_text)
                    + (ORDER_REGEX.findall(html_body) if html_body else [])
                )
            )
            if not order_ids:
                _LOGGER.debug(
                    "No order numbers found in email (subject: %s). "
                    "Checked plain text (%d chars) and HTML (%d chars).",
                    subject,
                    len(body_text),
                    len(html_body) if html_body else 0,
                )
                continue

            tracking_url = self._extract_tracking_url(html_body) if html_body else None

            updated_ts = (msg_datetime if msg_datetime is not None else received_utc).isoformat()
            did_update = False
            for order_id in order_ids:
                # Only overwrite if we don't have this order or this email is newer than stored
                existing = self._orders.get(order_id)
                if existing:
                    try:
                        existing_updated = _to_utc(
                            datetime.fromisoformat(existing["updated"])
                        )
                        if received_utc < existing_updated:
                            _LOGGER.debug(
                                "Order %s: skipping (email %s older than stored)",
                                order_id,
                                received_utc,
                            )
                            continue
                    except (ValueError, TypeError):
                        pass

                self._orders[order_id] = {
                    "status": status,
                    "subject": subject,
                    "updated": updated_ts,
                    "tracking_url": tracking_url,
                }
                did_update = True
                _LOGGER.debug(
                    "Order %s → %s (%s) [tracking: %s]",
                    order_id,
                    status,
                    subject,
                    tracking_url,
                )

            if mark_as_read:
                _LOGGER.debug("Marking email %s as read (order email processed)", num)
                mail.store(num, "+FLAGS", "\\Seen")

        mail.logout()

    def _status_from_subject(self, subject: str) -> str | None:
        """Determine order status from email subject."""
        for key, value in STATUS_MAP.items():
            if key in subject:
                return value
        return None

    def _decode_header(self, value: str) -> str:
        """Decode email headers safely."""
        parts = decode_header(value)
        decoded = ""
        for text, encoding in parts:
            if isinstance(text, bytes):
                decoded += text.decode(encoding or "utf-8", errors="ignore")
            else:
                decoded += text
        return decoded

    def _extract_text(self, msg) -> str:
        """Extract flattened text from email."""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() in ("text/plain", "text/html"):
                    payload = part.get_payload(decode=True)
                    if payload:
                        return payload.decode(errors="ignore")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                return payload.decode(errors="ignore")
        return ""

    def _extract_html(self, msg) -> str:
        """Extract HTML body from email."""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/html":
                    payload = part.get_payload(decode=True)
                    if payload:
                        return payload.decode(errors="ignore")
        else:
            if msg.get_content_type() == "text/html":
                payload = msg.get_payload(decode=True)
                if payload:
                    return payload.decode(errors="ignore")
        return ""

    def _extract_tracking_url(self, html_body: str) -> str | None:
        """Extract Amazon tracking URL or order link from email HTML."""
        soup = BeautifulSoup(html_body, "html.parser")

        for link in soup.find_all("a", href=True):
            text = link.get_text(" ", strip=True).lower()
            href = html.unescape(link["href"])

            # Case 1: Tracking links
            if "track package" in text or "progress-tracker" in href:
                match = re.search(
                    r"https://www\.amazon\.com/progress-tracker/[^&\"]+",
                    href,
                )
                if match:
                    return match.group(0)
                return href

            # Case 2: Order management links
            if "your-orders" in href and ("view" in text or "edit order" in text):
                return href

        return None
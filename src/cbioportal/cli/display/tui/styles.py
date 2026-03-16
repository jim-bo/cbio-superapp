"""prompt_toolkit styles for the cbio TUI."""
from prompt_toolkit.styles import Style

CBIO_CYAN = "#00B8D9"

STYLE = Style.from_dict({
    "title":            f"{CBIO_CYAN} bold",
    "meta":             "#888888",
    "separator":        "#444444",
    "prompt-marker":    f"{CBIO_CYAN} bold",
    "history-user":     "#FFFFFF",
    "history-command":  CBIO_CYAN,
    "history-response": "#888888",
    "notification":     "#FFAA00",
    "spinner":          CBIO_CYAN,
    "status-bar":           "bg:#1a1a1a #666666",
    "status-bar.mode":      f"bg:#1a1a1a {CBIO_CYAN}",
    "frame.border":         CBIO_CYAN,
    "welcome-heading":      CBIO_CYAN,
    "welcome-border":       CBIO_CYAN,
    "selector-active":  f"{CBIO_CYAN} bold",
    "selector-option":  "#888888",
    "selector-hint":    "#444444",
    "table-header":     f"{CBIO_CYAN} bold",
    "table-id":         CBIO_CYAN,
    "table-name":       "#FFFFFF",
    "table-samples":    "#888888",
    "table-cancer":     "#888888",
    "table-sep":        "#444444",
    "step-done":        "#888888",
    "step-value":       CBIO_CYAN,
    "success":          "#00CC66",
    "error":            "#FF4444",
    "warn":             "#FFAA00",
})

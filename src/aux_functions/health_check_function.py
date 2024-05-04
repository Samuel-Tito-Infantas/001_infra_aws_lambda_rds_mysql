def is_health_check_enabled(event):
    return event.get("health_check", {}).get("enabled", True)


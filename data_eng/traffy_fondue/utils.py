from datetime import datetime
import pytz


def convert_to_utc(timestamp_str):
    if not timestamp_str:
        return timestamp_str

    # Check if the timestamp is already in UTC (+00)
    if timestamp_str.endswith("+00"):
        return timestamp_str  # Return the timestamp as-is if it's already in UTC

    # Define the Thai timezone (GMT+7)
    thai_tz = pytz.timezone('Asia/Bangkok')

    # Parse the timestamp string (assumed format: "2025-01-16 06:08:25")
    local_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

    # Localize the time to Thai timezone (GMT+7)
    localized_time = thai_tz.localize(local_time)

    # Convert to UTC
    utc_time = localized_time.astimezone(pytz.utc)

    # Format to the desired format: "2025-01-16 02:53:34.290375+00"
    return utc_time.strftime("%Y-%m-%d %H:%M:%S.%f+00")

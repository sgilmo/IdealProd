#!/usr/bin/env python3
"""Network connectivity checker for multiple IP addresses."""

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from subprocess import PIPE, Popen
from typing import Dict, List, Optional
import json
import platform
import common_funcs

failed_devices = []
EMAIL_RECIPIENTS = ["elab@idealtridon.com"]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('c:\\PycharmProjects\\IdealProd\\network_check.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class NetworkDevice:
    """Represents a network device with its status."""
    name: str
    ip_address: str
    status: str = "Unknown"
    response_time: Optional[float] = None

    def __str__(self) -> str:
        status_str = f"{self.status}"
        if self.response_time is not None:
            status_str += f" ({self.response_time:.2f}ms)"
        return f"{self.name} at {self.ip_address}: {status_str}"

class NetworkChecker:
    """Handles network connectivity checking operations."""

    def __init__(self, config_file: str = "c:\\PycharmProjects\\IdealProd\\network_devices.json"):
        self.config_file = Path(config_file)
        self.devices: List[NetworkDevice] = []
        self.ping_count = 1
        self.timeout = 2000  # milliseconds
        self._load_config()

    def _load_config(self) -> None:
        """Load device configuration from JSON file."""
        try:
            with open(self.config_file, 'r') as f:
                devices_dict: Dict[str, str] = json.load(f)
                self.devices = [
                    NetworkDevice(name=name, ip_address=ip)
                    for name, ip in devices_dict.items()
                ]
        except FileNotFoundError:
            logger.error(f"Configuration file {self.config_file} not found")
            raise

    def _get_ping_command(self, ip_address: str) -> List[str]:
        """Get the appropriate ping command based on the operating system."""

        if platform.system().lower() == "windows":
            return ["ping", "-n", str(self.ping_count), "-w", str(self.timeout), ip_address]
        return ["ping", "-c", str(self.ping_count), "-W", str(self.timeout // 1000), ip_address]

    def check_device(self, device: NetworkDevice) -> None:
        """Check connectivity for a single device."""
        cmd = self._get_ping_command(device.ip_address)
        try:
            process = Popen(cmd, stdout=PIPE, stderr=PIPE, text=True)
            output, error = process.communicate()

            if process.returncode == 0:
                if "bytes=32 " in output:
                    # Extract response time if available
                    try:
                        time_str = output.split("time=")[1].split()[0].replace("ms", "")
                        device.response_time = float(time_str)
                    except (IndexError, ValueError):
                        pass
                    device.status = "Online"
                else:
                    device.status = "Unknown"
                    failed_devices.append(device.name + ' At ' + device.ip_address + ' Status Unknown')
            else:
                device.status = "Offline"
                failed_devices.append(device.name + ' At ' + device.ip_address + ' is Offline')

        except Exception as e:
            logger.error(f"Error checking {device.name}: {str(e)}")
            device.status = "Error"

    def check_all_devices(self) -> None:
        """Check connectivity for all configured devices."""
        start_time = datetime.now()
        logger.info(f"Starting network check for {len(self.devices)} devices...")

        for device in self.devices:
            self.check_device(device)

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Network check completed in {duration:.2f} seconds")

    def get_offline_devices(self) -> List[NetworkDevice]:
        """Return list of offline devices."""
        return [device for device in self.devices if device.status != "Online"]

    def print_results(self) -> None:
        """Print check results in a formatted way."""
        print("\nNetwork Check Results:")
        print("-" * 50)
        
        # Group devices by status
        status_groups: Dict[str, List[NetworkDevice]] = {}
        for device in self.devices:
            status_groups.setdefault(device.status, []).append(device)

        # Print results grouped by status
        for status in ["Online", "Offline", "Unknown", "Error"]:
            if status in status_groups:
                print(f"\n{status} devices:")
                for device in status_groups[status]:
                    print(f"  {device}")

        # Print summary
        print("\nSummary:")
        for status in status_groups:
            print(f"{status}: {len(status_groups[status])} devices")


def notify_failed_devices(failed) -> None:
    """Send notification about failed devices."""
    if not failed:
        return

    print(f"{len(failed)} Bad Connections {failed}")
    failure_report = "<br>".join(str(device) for device in failed)

    common_funcs.build_email2(
        body_data=failure_report,
        subject="Network Connection Failure",
        message_header="The Following Devices are off the network:\n\n",
        mailto=EMAIL_RECIPIENTS
    )

def main():
    """Main function."""
    try:
        checker = NetworkChecker()
        checker.check_all_devices()
        checker.print_results()
        # Get offline devices
        offline_devices = checker.get_offline_devices()
        if offline_devices:
            logger.warning(f"Found {len(offline_devices)} offline devices")
            notify_failed_devices(failed_devices)
            # Here you could add email notification or other alerts
        else:
            logger.info("All devices are online")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
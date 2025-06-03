"""
ACM Set Script - Communicates with ACM machines to set DM memory values
using FINS UDP protocol.
"""
import omronfins.finsudp as finsudp
from omronfins.finsudp import datadef
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('acm_set.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Create dictionary structure for machine IP addresses
MACH_DICT = {'ACM365': ['10.143.50.56', 9609], 'ACM366': ['10.143.50.58', 9610], 'ACM361': ['10.143.50.50', 9606],
            'ACM362': ['10.143.50.52', 9607], 'LACM384': ['10.143.50.20', 9622], 'LACM383': ['10.143.50.22', 9621],
            'ACM372': ['10.143.50.68', 9614], 'ACM374': ['10.143.50.72', 9616], 'LACM390': ['10.143.50.26', 9626],
            'LACM391': ['10.143.50.30', 9627], 'LACM381': ['10.143.50.14', 9619], 'LACM382': ['10.143.50.16', 9620],
            'LACM386': ['10.143.50.18', 9624], 'LACM388': ['10.143.50.86', 9600]
            }

# FINS UDP configuration constants
LOCAL_NET_ADDR = 0
LOCAL_NODE_NUM = 100
DEST_NET_ADDR = 0
DEST_NODE_NUM = 0
DEST_UNIT_ADDR = 0

# Memory area constants
HR_ADDRESS = 1
HR_BIT = 2
# Chopper Enabled HR_VALUE = 0, Disabled HR_VALUE = 1
HR_VALUE = 0


def main():
    """
    Main function to communicate with ACM machines and set DM memory values.
    """
    fins = finsudp.FinsUDP(LOCAL_NET_ADDR, LOCAL_NODE_NUM)
    fins.set_destination(dst_net_addr=DEST_NET_ADDR, dst_node_num=DEST_NODE_NUM, dst_unit_addr=DEST_UNIT_ADDR)
    
    failed_machines = []
    
    try:
        for key, value in MACH_DICT.items():
            ip_address = value[0]
            port = value[1]
            
            try:
                logger.info(f"Connecting to {key} at {ip_address}:{port}")
                ret1 = fins.open(ip_address, port)
                
                if ret1 != 0:
                    logger.error(f"Failed to connect to {key}: Error code {hex(ret1)}")
                    failed_machines.append(key)
                    continue

                ret2 = fins.write_mem_area(datadef.HR_BIT, HR_ADDRESS, HR_BIT, 1, (HR_VALUE, datadef.BIT))
                
                if ret2 != 0:
                    logger.error(f"Failed to write to {key} memory: Error code {hex(ret2)}")
                    failed_machines.append(key)
                else:
                    logger.info(f"Successfully set {key} DM{HR_ADDRESS} to {HR_VALUE}")
                
            except Exception as e:
                logger.error(f"Error communicating with {key}: {str(e)}")
                failed_machines.append(key)
            finally:
                # Always try to close the connection
                try:
                    fins.close()
                except Exception as e:
                    logger.warning(f"Error closing connection to {key}: {str(e)}")
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    
    # Report results
    if failed_machines:
        logger.warning(f"Failed to communicate with {len(failed_machines)} machines: {failed_machines}")
    else:
        logger.info("Successfully communicated with all machines")


if __name__ == "__main__":
    main()
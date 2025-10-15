import as400
import sql_funcs


def main():
    # Main Function
    orders = as400.get_orders()
    sql_funcs.update_orders(orders)
    return

if __name__ == '__main__':
        main()
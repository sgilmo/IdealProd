import as400
import sql_funcs


def main():
    # Main Function
    orders = as400.get_orders()
    sql_funcs.update_orders(orders)

    all_orders = as400.get_comp_orders()
    sql_funcs.update_all_orders(all_orders)
    return

if __name__ == '__main__':
        main()
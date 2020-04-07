-- Provide your own location to the table here

load lineitem /home/bala/projects/data/tpch/lineitem.tbl | NULL;
load nation /home/bala/projects/data/tpch/nation.tbl | NULL;
load part /home/bala/projects/data/tpch/part.tbl | NULL;
load partsupp /home/bala/projects/data/tpch/partsupp.tbl | NULL;
load supplier /home/bala/projects/data/tpch/supplier.tbl | NULL;
load region /home/bala/projects/data/tpch/region.tbl | NULL;
load orders /home/bala/projects/data/tpch/orders.tbl | NULL;
load customer /home/bala/projects/data/tpch/customer.tbl | NULL;
compress;

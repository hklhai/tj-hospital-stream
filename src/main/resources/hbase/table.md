

## 在hbase中创建namespace和table,默认在default下
create_namespace 'hk_flink'

create 'hk_flink:users',{NAME=>'F',BLOCKCACHE=>true,BLOOMFILTER=>'ROW', BLOCKSIZE => '65536'}

scan 'hk_flink:users'
count 'hk_flink:users'



## 商品表
create 'hk_flink:goods',{NAME=>'F',BLOCKCACHE=>true,BLOOMFILTER=>'ROW',DATA_BLOCK_ENCODING=>'PREFIX_TREE',BLOCKSIZE => '65536'}
create 'hk_flink:goods',{NAME=>'F',BLOCKCACHE=>true,BLOOMFILTER=>'ROW', BLOCKSIZE => '65536'}


## 订单表
create 'hk_flink:orders',{NAME=>'0',BLOCKCACHE=>true,BLOOMFILTER=>'ROW', DATA_BLOCK_ENCODING => 'PREFIX_TREE', BLOCKSIZE => '65536'}
create 'hk_flink:orders',{NAME=>'0',BLOCKCACHE=>true,BLOOMFILTER=>'ROW', BLOCKSIZE => '65536'}




CREATE TABLE  goods (
	goodsId INT(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
	goodsName varchar(50) NOT NULL COMMENT '商品名称',
	sellingPrice DECIMAL(11,2) DEFAULT 0.00 NOT NULL COMMENT '售价',
	goodsStock INT(11) DEFAULT 0 NOT NULL COMMENT '商品总库存',
	appraiseNum INT(11) DEFAULT 0 NOT NULL COMMENT '评价数',
	CONSTRAINT dajiangtai_goods_PK PRIMARY KEY (goodsId)
)ENGINE=InnoDB DEFAULT CHARSET=utf8




CREATE TABLE  orders (
  orderId int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  orderNo varchar(50) NOT NULL COMMENT '订单号',
  userId int(11) NOT NULL COMMENT '用户ID',
  goodId int(11) NOT NULL COMMENT '商品ID',
  goodsMoney decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '商品总金额',
  realTotalMoney decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '实际订单总金额',
  payFrom int(11) NOT NULL DEFAULT '0' COMMENT '支付来源(1:支付宝，2：微信)',
  province varchar(50) NOT NULL COMMENT '省份',
  createTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`orderId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8




CREATE TABLE  orders (
  orderId int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  orderNo varchar(50) NOT NULL COMMENT '订单号',
  userId int(11) NOT NULL COMMENT '用户ID',
  goodId int(11) NOT NULL COMMENT '商品ID',
  goodsMoney decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '商品总金额',
  realTotalMoney decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '实际订单总金额',
  payFrom int(11) NOT NULL DEFAULT '0' COMMENT '支付来源(1:支付宝，2：微信)',
  province varchar(50) NOT NULL COMMENT '省份',
  createTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`orderId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
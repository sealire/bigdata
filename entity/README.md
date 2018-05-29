数据模型模块

IntData.java整数类，对应表int_data
表结构：
CREATE TABLE `int_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `number` int(11) DEFAULT '0' COMMENT '整数值',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='整数表';

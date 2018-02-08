# kudu-mapper
##目标：java操作kudu 能有mybatis一样方便

- 连接kudu 集群的配置在 com.linlinj.db.kudu.utils.KuduTemplate.java文件中，因我们配置主要交给上层项目，因此kudu作为组件自己不单独用配置
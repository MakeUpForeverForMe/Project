package com.weshare.bizconftable.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.mapper.MapperScannerConfigurer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.sql.DataSource;

/**
 * created by chao.guo on 2021/2/3
 **/
@Configuration
public class MyBatisConfig {
    @Bean(name = "mysqlSessionFactory")
    public SqlSessionFactory sqlMySqlSessionFactory(@Qualifier("mysqlDataSource") DataSource mysqlDataSource) throws Exception {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(mysqlDataSource);
        bean.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources("classpath:mapper/mysql/*Mapper.xml"));
        /*bean.setTypeAliasesPackage("com.weshare.bizconftable.bean");*/
        return bean.getObject();
    }

    @Bean(name = "hiveSessionFactory")
    @Primary
    public SqlSessionFactory sqlhiveSessionFactory(@Qualifier("hiveDataSource") DataSource mysqlDataSource) throws Exception {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(mysqlDataSource);
        bean.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources("classpath:mapper/hive/*Mapper.xml"));
       /* bean.setTypeAliasesPackage("com.weshare.bizconftable.bean");*/

        return bean.getObject();
    }

    @Bean(name = "mysqlMapperScanner")
    public MapperScannerConfigurer mysqlMapperScannerConfigurer() {
        MapperScannerConfigurer mapperScannerConfigurer = new MapperScannerConfigurer();
        mapperScannerConfigurer.setBasePackage("com.weshare.bizconftable.mapper.mysql");
        mapperScannerConfigurer.setSqlSessionFactoryBeanName("mysqlSessionFactory");
        mapperScannerConfigurer.setSqlSessionTemplateBeanName("mysqlSqlSessionTemplate");
        return mapperScannerConfigurer;
    }

    @Bean(name = "hiveMapperScanner")
    @Primary
    public MapperScannerConfigurer hiveMapperScannerConfigurer() {
        MapperScannerConfigurer mapperScannerConfigurer = new MapperScannerConfigurer();
        mapperScannerConfigurer.setBasePackage("com.weshare.bizconftable.mapper.hive");
        mapperScannerConfigurer.setSqlSessionFactoryBeanName("hiveSessionFactory");
        mapperScannerConfigurer.setSqlSessionTemplateBeanName("hiveSqlSessionTemplate");
        return mapperScannerConfigurer;
    }
    @Bean("hiveSqlSessionTemplate")
    @Primary
    public SqlSessionTemplate hiveSqlSessionTemplate(@Qualifier("hiveSessionFactory") SqlSessionFactory sqlSessionFactory){
        SqlSessionTemplate sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
        sqlSessionTemplate.getConfiguration().setMapUnderscoreToCamelCase(true);
        return sqlSessionTemplate;
    }

    @Bean("mysqlSqlSessionTemplate")
    public SqlSessionTemplate mysqlSqlSessionTemplate(@Qualifier("mysqlSessionFactory") SqlSessionFactory sqlSessionFactory){
        SqlSessionTemplate sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
        sqlSessionTemplate.getConfiguration().setMapUnderscoreToCamelCase(true);
        return sqlSessionTemplate;
    }
}

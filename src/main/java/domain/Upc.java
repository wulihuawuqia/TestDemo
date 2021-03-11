package domain;

import cn.hutool.core.util.RandomUtil;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * @program: TestSe
 * @description: UPC属性
 * @author: wuqia
 * @date: 2021-03-08 20:12
 **/
@Data
public class Upc implements Serializable {

    /**
     * 产品条码 8/13
     */
    private String baseGting;

    /**
     * 条码状态
     */
    private Integer baseStatus;

    /**
     * 产品名称
     */
    private String descRiption;

    /**
     * 品牌
     */
    private String Brand;

    /**
     * 产品规格
     */
    private String Specification;

    private BigDecimal price;

    private Date SaleDate;

    private String UnsPsc;

    private String gpc;

    /**
     * 企业名称
     */
    private String firmName;

    /**
     * 图片地址
     */
    private String PictrueFileName;

    /**
     * 净含量数值
     */
    private String NetContent;

    /**
     * 净含量单位代码
     */
    private String NetCongtenUnit;

    public static Upc getInstance() {
        Upc upc = new Upc();
        upc.setBaseGting(RandomUtil.randomString(10));
        upc.setBaseStatus(RandomUtil.randomInt(4));
        upc.setDescRiption(RandomUtil.randomString(20));
        upc.setDescRiption(RandomUtil.randomString(20));
        upc.setSpecification(RandomUtil.randomString(20));
        upc.setPrice(RandomUtil.randomBigDecimal());
        upc.setSaleDate(new Date());
        upc.setUnsPsc(RandomUtil.randomString(6));
        upc.setGpc(RandomUtil.randomString(1));
        upc.setFirmName(RandomUtil.randomString(10));
        upc.setPictrueFileName(RandomUtil.randomString(20));
        upc.setNetContent(RandomUtil.randomString(10));
        upc.setNetCongtenUnit(RandomUtil.randomString(4));
        return upc;
    }


}

/*
Navicat MySQL Data Transfer

Source Server         : mysql
Source Server Version : 50537
Source Host           : localhost:3306
Source Database       : spark

Target Server Type    : MYSQL
Target Server Version : 50537
File Encoding         : 65001

Date: 2016-03-26 22:09:00
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for dthadoop
-- ----------------------------
DROP TABLE IF EXISTS `dthadoop`;
CREATE TABLE `dthadoop` (
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `age` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Records of dthadoop
-- ----------------------------
INSERT INTO `dthadoop` VALUES ('Michael', '12');
INSERT INTO `dthadoop` VALUES ('Andy', '23');
INSERT INTO `dthadoop` VALUES ('Justin', '34');
INSERT INTO `dthadoop` VALUES ('spark', '99');

-- ----------------------------
-- Table structure for dtspark
-- ----------------------------
DROP TABLE IF EXISTS `dtspark`;
CREATE TABLE `dtspark` (
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `score` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Records of dtspark
-- ----------------------------
INSERT INTO `dtspark` VALUES ('Michael', '45');
INSERT INTO `dtspark` VALUES ('Andy', '100');
INSERT INTO `dtspark` VALUES ('Justin', '91');
INSERT INTO `dtspark` VALUES ('spark', '99');

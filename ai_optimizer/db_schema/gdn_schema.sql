-- MySQL dump 10.16  Distrib 10.1.38-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: dev_gdn
-- ------------------------------------------------------
-- Server version	10.1.38-MariaDB-0ubuntu0.18.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `adgroup_initial_bid`
--

DROP TABLE IF EXISTS `adgroup_initial_bid`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adgroup_initial_bid` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adgroup_insights`
--

DROP TABLE IF EXISTS `adgroup_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adgroup_insights` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `channel_type` varchar(255) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `cpm_bid` float DEFAULT NULL,
  `cpv_bid` float DEFAULT NULL,
  `cpc_bid` float DEFAULT NULL,
  `cpa_bid` float DEFAULT NULL,
  `bidding_type` varchar(255) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `clicks` int(11) DEFAULT NULL,
  `conversions` int(11) DEFAULT NULL,
  `cost_per_click` float DEFAULT NULL,
  `cost_per_conversion` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adgroup_score`
--

DROP TABLE IF EXISTS `adgroup_score`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adgroup_score` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `age_range_insights`
--

DROP TABLE IF EXISTS `age_range_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `age_range_insights` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `age_range` varchar(255) DEFAULT NULL,
  `criterion_id` int(11) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `cpm_bid` float DEFAULT NULL,
  `cpc_bid` float DEFAULT NULL,
  `bidding_type` varchar(255) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `clicks` int(11) DEFAULT NULL,
  `conversions` int(11) DEFAULT NULL,
  `cost_per_click` float DEFAULT NULL,
  `cost_per_conversion` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `age_range_score`
--

DROP TABLE IF EXISTS `age_range_score`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `age_range_score` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `age_range` varchar(255) DEFAULT NULL,
  `criterion_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `audience_insights`
--

DROP TABLE IF EXISTS `audience_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `audience_insights` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `audience` varchar(255) DEFAULT NULL,
  `criterion_id` bigint(11) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `cpm_bid` float DEFAULT NULL,
  `cpc_bid` float DEFAULT NULL,
  `bidding_type` varchar(255) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `clicks` int(11) DEFAULT NULL,
  `conversions` int(11) DEFAULT NULL,
  `cost_per_click` float DEFAULT NULL,
  `cost_per_conversion` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `audience_score`
--

DROP TABLE IF EXISTS `audience_score`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `audience_score` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `audience` varchar(255) DEFAULT NULL,
  `criterion_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_target`
--

DROP TABLE IF EXISTS `campaign_target`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_target` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `channel_type` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `destination_type` varchar(255) DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `daily_budget` float DEFAULT NULL,
  `daily_target` float DEFAULT NULL,
  `destination` float DEFAULT NULL,
  `period` int(11) DEFAULT NULL,
  `period_left` int(11) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `spend_cap` float DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `stop_time` datetime DEFAULT NULL,
  `target` int(11) DEFAULT NULL,
  `target_left` int(11) DEFAULT NULL,
  `bidding_type` varchar(255) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `clicks` int(11) DEFAULT NULL,
  `conversions` int(11) DEFAULT NULL,
  `cost_per_click` float DEFAULT NULL,
  `cost_per_conversion` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `criteria_insights`
--

DROP TABLE IF EXISTS `criteria_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `criteria_insights` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `keyword_placement` varchar(255) DEFAULT NULL,
  `keyword_id` bigint(11) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `cpm_bid` float DEFAULT NULL,
  `cpc_bid` float DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `clicks` int(11) DEFAULT NULL,
  `conversions` int(11) DEFAULT NULL,
  `cost_per_click` float DEFAULT NULL,
  `cost_per_conversion` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `criteria_score`
--

DROP TABLE IF EXISTS `criteria_score`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `criteria_score` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `keyword_placement` varchar(255) DEFAULT NULL,
  `keyword_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `display_keyword_insights`
--

DROP TABLE IF EXISTS `display_keyword_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `display_keyword_insights` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `keyword` varchar(255) DEFAULT NULL,
  `keyword_id` bigint(20) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `cpm_bid` float DEFAULT NULL,
  `cpc_bid` float DEFAULT NULL,
  `bidding_type` varchar(255) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `clicks` int(11) DEFAULT NULL,
  `conversions` int(11) DEFAULT NULL,
  `cost_per_click` float DEFAULT NULL,
  `cost_per_conversion` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `display_keyword_score`
--

DROP TABLE IF EXISTS `display_keyword_score`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `display_keyword_score` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `keyword` varchar(255) DEFAULT NULL,
  `keyword_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `optimal_weight`
--

DROP TABLE IF EXISTS `optimal_weight`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `optimal_weight` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `weight_kpi` float DEFAULT NULL,
  `weight_spend` float DEFAULT NULL,
  `weight_bid` float DEFAULT NULL,
  `score` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `release_ver_result`
--

DROP TABLE IF EXISTS `release_ver_result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `release_ver_result` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `result` blob,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `result`
--

DROP TABLE IF EXISTS `result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `result` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `result` blob,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `url_insights`
--

DROP TABLE IF EXISTS `url_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `url_insights` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `clicks` int(11) DEFAULT NULL,
  `conversions` int(11) DEFAULT NULL,
  `cost_per_click` float DEFAULT NULL,
  `cost_per_conversion` float DEFAULT NULL,
  `url_display_name` mediumtext,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `url_score`
--

DROP TABLE IF EXISTS `url_score`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `url_score` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `url_display_name` varchar(255) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-05-10 12:29:43

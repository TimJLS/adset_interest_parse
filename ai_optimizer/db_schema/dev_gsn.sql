-- MySQL dump 10.16  Distrib 10.1.41-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: dev_gsn
-- ------------------------------------------------------
-- Server version	10.1.41-MariaDB-0ubuntu0.18.04.1

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
  `keyword_id` bigint(20) DEFAULT NULL,
  `bid_amount` float DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `adgroup_id` (`adgroup_id`,`keyword_id`),
  KEY `campaign_id` (`campaign_id`)
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
-- Table structure for table `ai_behavior_log`
--

DROP TABLE IF EXISTS `ai_behavior_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_behavior_log` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `criterion_id` bigint(20) DEFAULT NULL,
  `display_name` varchar(200) DEFAULT NULL,
  `criterion_type` varchar(50) DEFAULT NULL,
  `behavior` varchar(20) DEFAULT NULL,
  `behavior_misc` varchar(20) DEFAULT NULL,
  `created_at` int(10) DEFAULT NULL,
  KEY `campaign_id` (`campaign_id`,`adgroup_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_insights`
--

DROP TABLE IF EXISTS `campaign_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_insights` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `channel_type` double DEFAULT NULL,
  `status` double DEFAULT NULL,
  `bidding_type` text,
  `daily_budget` bigint(20) DEFAULT NULL,
  `start_time` double DEFAULT NULL,
  `stop_time` double DEFAULT NULL,
  `spend` double DEFAULT NULL,
  `cost_per_target` double DEFAULT NULL,
  `impressions` bigint(20) DEFAULT NULL,
  `clicks` bigint(20) DEFAULT NULL,
  `conversions` double DEFAULT NULL,
  `cost_per_click` double DEFAULT NULL,
  `cost_per_conversion` double DEFAULT NULL,
  `ctr` double DEFAULT NULL,
  `average_position` double DEFAULT NULL,
  `top_impression_percentage` float NOT NULL
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
  `ai_status` varchar(20) DEFAULT NULL,
  `destination_type` varchar(255) DEFAULT NULL,
  `is_optimized` varchar(255) DEFAULT NULL,
  `optimized_date` date DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `daily_budget` float DEFAULT NULL,
  `daily_target` float DEFAULT NULL,
  `destination` float DEFAULT NULL,
  `destination_max` int(11) DEFAULT NULL,
  `period` int(11) DEFAULT NULL,
  `period_left` int(11) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `ai_spend_cap` float DEFAULT NULL,
  `ai_start_date` date DEFAULT NULL,
  `ai_stop_date` date DEFAULT NULL,
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
  `cost_per_conversion` float DEFAULT NULL,
  `all_conversions` int(11) DEFAULT NULL,
  `cost_per_all_conversion` float DEFAULT NULL,
  `average_position` float DEFAULT NULL,
  `top_impression_percentage` float NOT NULL,
  `is_smart_spending` enum('True','False') NOT NULL DEFAULT 'True',
  `is_lookalike` enum('True','False') NOT NULL DEFAULT 'True',
  `is_target_suggest` enum('True','False') NOT NULL DEFAULT 'True',
  `is_creative_opt` enum('True','False') NOT NULL DEFAULT 'True',
  UNIQUE KEY `campaign_id` (`campaign_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_target_test`
--

DROP TABLE IF EXISTS `campaign_target_test`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_target_test` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `channel_type` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `ai_status` varchar(20) DEFAULT NULL,
  `destination_type` varchar(255) DEFAULT NULL,
  `is_optimized` varchar(255) DEFAULT NULL,
  `optimized_date` date DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `daily_budget` float DEFAULT NULL,
  `daily_target` float DEFAULT NULL,
  `destination` float DEFAULT NULL,
  `period` int(11) DEFAULT NULL,
  `period_left` int(11) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `ai_spend_cap` float DEFAULT NULL,
  `ai_start_date` date DEFAULT NULL,
  `ai_stop_date` date DEFAULT NULL,
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
  `cost_per_conversion` float DEFAULT NULL,
  `all_conversions` int(11) DEFAULT NULL,
  `cost_per_all_conversion` float DEFAULT NULL,
  `average_position` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `custom_audience`
--

DROP TABLE IF EXISTS `custom_audience`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `custom_audience` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) NOT NULL,
  `behavior_type` enum('AllConverters','OptimizedList') NOT NULL,
  `list_type` enum('Similar','CustomAudience') NOT NULL,
  `display_name` varchar(255) NOT NULL,
  `is_created` enum('True','False') DEFAULT NULL,
  `size` int(11) DEFAULT NULL,
  `membership_life_span` int(11) NOT NULL,
  `criterion_id` bigint(20) DEFAULT NULL,
  `is_in_adgroup` enum('True','False') NOT NULL DEFAULT 'False',
  `seed_id` bigint(20) NOT NULL,
  `seed_list_type` enum('logical','remarketing','rule_based','similar','None') NOT NULL DEFAULT 'None',
  `seed_size` int(11) NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UNIQUE_FIELDS` (`campaign_id`,`behavior_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `keyword_insights`
--

DROP TABLE IF EXISTS `keyword_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `keyword_insights` (
  `adgroup_id` bigint(20) DEFAULT NULL,
  `bidding_type` text,
  `campaign_id` bigint(20) DEFAULT NULL,
  `clicks` bigint(20) DEFAULT NULL,
  `conversions` double DEFAULT NULL,
  `cost_per_click` double DEFAULT NULL,
  `cost_per_conversion` double DEFAULT NULL,
  `cost_per_target` double DEFAULT NULL,
  `cpc_bid` double DEFAULT NULL,
  `cpm_bid` double DEFAULT NULL,
  `ctr` double DEFAULT NULL,
  `customer_id` bigint(20) DEFAULT NULL,
  `first_page_cpc` double DEFAULT NULL,
  `impressions` bigint(20) DEFAULT NULL,
  `keyword` text,
  `keyword_id` double DEFAULT NULL,
  `position` double DEFAULT NULL,
  `serving_status` text,
  `spend` double DEFAULT NULL,
  `status` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `keywords_insights`
--

DROP TABLE IF EXISTS `keywords_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `keywords_insights` (
  `customer_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `keyword` varchar(255) DEFAULT NULL,
  `keyword_id` bigint(20) DEFAULT NULL,
  `position` float DEFAULT NULL,
  `serving_status` varchar(255) DEFAULT NULL,
  `first_page_cpc` float DEFAULT NULL,
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
  `all_conversions` int(11) DEFAULT NULL,
  `cost_per_all_conversion` float DEFAULT NULL,
  `top_impression_percentage` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `keywords_score`
--

DROP TABLE IF EXISTS `keywords_score`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `keywords_score` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adgroup_id` bigint(20) DEFAULT NULL,
  `keyword` varchar(255) DEFAULT NULL,
  `keyword_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP,
  KEY `campaign_id` (`campaign_id`)
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
  `weight_ave_position` float DEFAULT NULL,
  `weight_first_page` float DEFAULT NULL,
  `weight_all_conv` float DEFAULT NULL,
  `weight_conv` float DEFAULT NULL,
  `weight_conv_rate` float DEFAULT NULL,
  `weight_kpi` float DEFAULT NULL,
  `weight_spend` float DEFAULT NULL,
  `weight_bid` float DEFAULT NULL,
  `score` float DEFAULT NULL
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
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-09-20 11:25:52

-- MySQL dump 10.16  Distrib 10.1.41-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: dev_facebook_test
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
-- Table structure for table `account_target_suggestion`
--

DROP TABLE IF EXISTS `account_target_suggestion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `account_target_suggestion` (
  `account_id` bigint(20) NOT NULL,
  `suggestion_id` bigint(20) NOT NULL,
  `suggestion_name` varchar(50) NOT NULL,
  `suggestion_type` varchar(50) NOT NULL,
  `audience_size` int(20) NOT NULL,
  `log_date` date NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_conversion_metrics`
--

DROP TABLE IF EXISTS `adset_conversion_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_conversion_metrics` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `add_to_cart` int(11) DEFAULT NULL,
  `initiate_checkout` int(11) DEFAULT NULL,
  `purchase` int(11) DEFAULT NULL,
  `view_content` int(11) DEFAULT NULL,
  `landing_page_view` int(11) DEFAULT NULL,
  `link_click` int(11) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `cost_per_purchase` float DEFAULT NULL,
  `cost_per_add_to_cart` float DEFAULT NULL,
  `cost_per_initiate_checkout` float DEFAULT NULL,
  `cost_per_view_content` float DEFAULT NULL,
  `cost_per_landing_page_view` float DEFAULT NULL,
  `cost_per_link_click` float DEFAULT NULL,
  `spend` int(11) DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `daily_budget` int(11) DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_creative_log`
--

DROP TABLE IF EXISTS `adset_creative_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_creative_log` (
  `campaign_id` bigint(20) NOT NULL,
  `adset_id` bigint(20) NOT NULL,
  `process_date` date NOT NULL,
  PRIMARY KEY (`adset_id`),
  KEY `adset_id` (`adset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_initial_bid`
--

DROP TABLE IF EXISTS `adset_initial_bid`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_initial_bid` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `campaign_id` bigint(20) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `adset_id` (`adset_id`),
  KEY `campaign_id` (`campaign_id`)
) ENGINE=InnoDB AUTO_INCREMENT=222982 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_insights`
--

DROP TABLE IF EXISTS `adset_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_insights` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `target` int(11) DEFAULT NULL,
  `charge` int(11) DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `cost_per_charge` float DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `daily_budget` int(11) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `request_time` datetime DEFAULT NULL,
  `age_max` int(11) DEFAULT NULL,
  `age_min` int(11) DEFAULT NULL,
  `flexible_spec` blob,
  `geo_locations` blob,
  `reach` int(11) DEFAULT NULL,
  `optimization_goal` varchar(255) DEFAULT NULL,
  KEY `campaign_id` (`campaign_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_leads_metrics`
--

DROP TABLE IF EXISTS `adset_leads_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_leads_metrics` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `leadgen.other` int(11) DEFAULT NULL,
  `fb_pixel_complete_registration` int(11) DEFAULT NULL,
  `fb_pixel_lead` int(11) DEFAULT NULL,
  `fb_pixel_view_content` int(11) DEFAULT NULL,
  `landing_page_view` int(11) DEFAULT NULL,
  `link_click` int(11) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `cost_per_leadgen.other` float DEFAULT NULL,
  `cost_per_fb_pixel_complete_registration` float DEFAULT NULL,
  `cost_per_fb_pixel_lead` float DEFAULT NULL,
  `cost_per_fb_pixel_view_content` float DEFAULT NULL,
  `cost_per_landing_page_view` float DEFAULT NULL,
  `cost_per_link_click` float DEFAULT NULL,
  `spend` int(11) DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `daily_budget` int(11) DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_metrics`
--

DROP TABLE IF EXISTS `adset_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_metrics` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `optimization_goal` varchar(50) NOT NULL,
  `actual_metrics` char(255) DEFAULT NULL,
  `awareness` int(11) DEFAULT NULL,
  `interest` int(11) DEFAULT NULL,
  `desire` int(11) DEFAULT NULL,
  `action` int(11) DEFAULT NULL,
  `landing_page_view` int(11) DEFAULT NULL,
  `link_click` int(11) DEFAULT NULL,
  `reach` int(11) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `spend` int(11) DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `daily_budget` int(11) DEFAULT NULL,
  `flexible_spec` text,
  `geo_locations` text,
  `age_max` int(11) DEFAULT NULL,
  `age_min` int(11) DEFAULT NULL,
  `status` varchar(50) DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY `campaign_id` (`campaign_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_score`
--

DROP TABLE IF EXISTS `adset_score`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_score` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_status_log`
--

DROP TABLE IF EXISTS `adset_status_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_status_log` (
  `campaign_id` bigint(20) NOT NULL,
  `adset_id` bigint(20) NOT NULL,
  `optimization_goal` varchar(50) DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `clicks` int(11) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `spend` int(11) DEFAULT NULL,
  `log_date` date DEFAULT NULL,
  `log_datetime` datetime DEFAULT NULL
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
  `adset_id` bigint(20) DEFAULT NULL,
  `adset_name` varchar(200) DEFAULT NULL,
  `behavior` varchar(20) DEFAULT NULL,
  `behavior_misc` varchar(20) DEFAULT NULL,
  `created_at` int(10) DEFAULT NULL,
  KEY `campaign_id` (`campaign_id`,`adset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_conversion_metrics`
--

DROP TABLE IF EXISTS `campaign_conversion_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_conversion_metrics` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `add_to_cart` int(11) DEFAULT NULL,
  `initiate_checkout` int(11) DEFAULT NULL,
  `purchase` int(11) DEFAULT NULL,
  `view_content` int(11) DEFAULT NULL,
  `landing_page_view` int(11) DEFAULT NULL,
  `link_click` int(11) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `cost_per_purchase` float DEFAULT NULL,
  `cost_per_add_to_cart` float DEFAULT NULL,
  `cost_per_initiate_checkout` float DEFAULT NULL,
  `cost_per_view_content` float DEFAULT NULL,
  `cost_per_landing_page_view` float DEFAULT NULL,
  `cost_per_link_click` float DEFAULT NULL,
  `spend` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_insights`
--

DROP TABLE IF EXISTS `campaign_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_insights` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `target` int(11) DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `reach` int(11) DEFAULT NULL,
  `request_time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_leads_metrics`
--

DROP TABLE IF EXISTS `campaign_leads_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_leads_metrics` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `leadgen.other` float DEFAULT NULL,
  `fb_pixel_complete_registration` float DEFAULT NULL,
  `fb_pixel_lead` float DEFAULT NULL,
  `fb_pixel_view_content` int(11) DEFAULT NULL,
  `landing_page_view` int(11) DEFAULT NULL,
  `link_click` int(11) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `cost_per_leadgen.other` float DEFAULT NULL,
  `cost_per_fb_pixel_view_content` float DEFAULT NULL,
  `cost_per_fb_pixel_lead` float DEFAULT NULL,
  `cost_per_fb_pixel_complete_registration` float DEFAULT NULL,
  `cost_per_landing_page_view` float DEFAULT NULL,
  `cost_per_link_click` float DEFAULT NULL,
  `spend` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_metrics`
--

DROP TABLE IF EXISTS `campaign_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_metrics` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `campaign_id` bigint(20) DEFAULT NULL,
  `actual_metrics` varchar(255) DEFAULT NULL,
  `charge_type` enum('LINK_CLICKS','POST_ENGAGEMENT','LANDING_PAGE_VIEW','VIDEO_VIEWS','CONVERSIONS','ADD_TO_CART','THRUPLAY','PAGE_LIKES','REACH','ALL_CLICKS','IMPRESSIONS','COMPLETE_REGISTRATION','VIEW_CONTENT','ADD_PAYMENT_INFO','ADD_TO_WISHLIST','LEAD_WEBSITE','LEAD_GENERATION','PURCHASES','INITIATE_CHECKOUT','SEARCH','MESSAGES') DEFAULT NULL,
  `awareness` int(11) DEFAULT NULL,
  `interest` int(11) DEFAULT NULL,
  `desire` int(11) DEFAULT NULL,
  `action` int(11) DEFAULT NULL,
  `landing_page_view` int(11) DEFAULT NULL,
  `link_click` int(11) DEFAULT NULL,
  `reach` int(11) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `spend` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `campaign_id` (`campaign_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=22892 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_optimal_weight`
--

DROP TABLE IF EXISTS `campaign_optimal_weight`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_optimal_weight` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `w_action` float DEFAULT NULL,
  `w_desire` float DEFAULT NULL,
  `w_interest` float DEFAULT NULL,
  `w_awareness` float DEFAULT NULL,
  `w_discovery` float DEFAULT NULL,
  `w_spend` float DEFAULT NULL,
  `w_bid` float DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_pixel_id`
--

DROP TABLE IF EXISTS `campaign_pixel_id`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_pixel_id` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `campaign_id` varchar(64) NOT NULL,
  `behavior_type` enum('AddToCart','ViewContent','Purchase','') NOT NULL,
  `pixel_id` varchar(64) NOT NULL,
  `is_created` enum('True','False') DEFAULT NULL,
  `operation_status` enum('Normal','Abnormal') NOT NULL DEFAULT 'Abnormal',
  `approximate_count` int(11) DEFAULT '1000',
  `retention_days` int(11) NOT NULL,
  `data_source` varchar(255) NOT NULL,
  `audience_id` varchar(64) DEFAULT NULL,
  `is_rt_in_adset` enum('True','False') NOT NULL DEFAULT 'False',
  `lookalike_audience_id` varchar(64) DEFAULT NULL,
  `is_lookalike_in_adset` enum('True','False') NOT NULL DEFAULT 'False',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UNIQUE_FIELDS` (`campaign_id`,`behavior_type`) USING BTREE,
  UNIQUE KEY `audience_id` (`audience_id`),
  UNIQUE KEY `lookalike_audience_id` (`lookalike_audience_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1646 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_predict_bids`
--

DROP TABLE IF EXISTS `campaign_predict_bids`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_predict_bids` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `bid_amount` float DEFAULT NULL,
  `predict_bids` varchar(255) DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY `campaign_id` (`campaign_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_target`
--

DROP TABLE IF EXISTS `campaign_target`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_target` (
  `account_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) NOT NULL,
  `destination` int(11) DEFAULT NULL,
  `destination_max` int(10) DEFAULT NULL,
  `charge_type` varchar(255) DEFAULT NULL,
  `destination_type` varchar(255) DEFAULT NULL,
  `custom_conversion_id` varchar(50) DEFAULT NULL,
  `is_optimized` varchar(255) DEFAULT NULL,
  `optimized_date` date DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `daily_budget` float DEFAULT NULL,
  `daily_charge` float DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `period` int(11) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `ai_spend_cap` int(11) DEFAULT NULL,
  `ai_start_date` date DEFAULT NULL,
  `ai_stop_date` date DEFAULT NULL,
  `ai_status` varchar(30) DEFAULT NULL,
  `spend_cap` int(11) DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `stop_time` datetime DEFAULT NULL,
  `target` int(11) DEFAULT NULL,
  `desire` int(11) DEFAULT NULL,
  `interest` int(11) DEFAULT NULL,
  `awareness` int(11) DEFAULT NULL,
  `target_left` int(11) DEFAULT NULL,
  `target_type` varchar(255) DEFAULT NULL,
  `reach` int(11) DEFAULT NULL,
  `is_smart_spending` enum('True','False') NOT NULL DEFAULT 'True',
  `is_target_suggest` enum('True','False') NOT NULL DEFAULT 'True',
  `is_lookalike` enum('True','False') NOT NULL DEFAULT 'True',
  `is_creative_opt` enum('True','False') NOT NULL DEFAULT 'True',
  `actual_metrics` char(255) DEFAULT NULL,
  PRIMARY KEY (`campaign_id`),
  KEY `campaign_id` (`campaign_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_target_suggestion`
--

DROP TABLE IF EXISTS `campaign_target_suggestion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_target_suggestion` (
  `account_id` bigint(20) NOT NULL,
  `campaign_id` bigint(20) NOT NULL,
  `source_adset_id` bigint(20) DEFAULT NULL,
  `suggest_id` bigint(20) NOT NULL,
  `suggest_name` varchar(50) NOT NULL,
  `audience_size` bigint(20) NOT NULL,
  PRIMARY KEY (`account_id`,`campaign_id`,`suggest_id`),
  KEY `campaign_id` (`campaign_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_target_test`
--

DROP TABLE IF EXISTS `campaign_target_test`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_target_test` (
  `account_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `destination` int(11) DEFAULT NULL,
  `charge_type` varchar(255) DEFAULT NULL,
  `is_optimized` varchar(255) DEFAULT NULL,
  `optimized_date` date DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `daily_budget` float DEFAULT NULL,
  `daily_charge` float DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `ctr` float DEFAULT NULL,
  `period` int(11) DEFAULT NULL,
  `spend` int(11) DEFAULT NULL,
  `ai_spend_cap` int(11) DEFAULT NULL,
  `ai_start_date` date DEFAULT NULL,
  `ai_stop_date` date DEFAULT NULL,
  `ai_status` varchar(30) NOT NULL,
  `spend_cap` int(11) DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `stop_time` datetime DEFAULT NULL,
  `target` int(11) DEFAULT NULL,
  `target_left` int(11) DEFAULT NULL,
  `target_type` varchar(255) DEFAULT NULL,
  `reach` int(11) DEFAULT NULL,
  KEY `campaign_id` (`campaign_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='for campaign_target testing';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `conversion_optimal_weight`
--

DROP TABLE IF EXISTS `conversion_optimal_weight`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `conversion_optimal_weight` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `w1` float DEFAULT NULL,
  `w2` float DEFAULT NULL,
  `w3` float DEFAULT NULL,
  `w4` float DEFAULT NULL,
  `w5` float DEFAULT NULL,
  `w6` float DEFAULT NULL,
  `w_spend` float DEFAULT NULL,
  `w_bid` float DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `facebook_adset_optimization_goal`
--

DROP TABLE IF EXISTS `facebook_adset_optimization_goal`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `facebook_adset_optimization_goal` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `adset_name` varchar(100) DEFAULT NULL,
  `pixel_rule` varchar(500) DEFAULT NULL,
  `pixel_id` bigint(20) DEFAULT NULL,
  `custom_event_type` varchar(100) DEFAULT NULL,
  `created_at` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `facebook_campaign_currency`
--

DROP TABLE IF EXISTS `facebook_campaign_currency`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `facebook_campaign_currency` (
  `campaign_id` bigint(20) NOT NULL,
  `currency` varchar(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `facebook_custom_conversion`
--

DROP TABLE IF EXISTS `facebook_custom_conversion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `facebook_custom_conversion` (
  `account_id` bigint(20) DEFAULT NULL,
  `campaign_id` bigint(20) DEFAULT NULL,
  `conversion_id` bigint(20) DEFAULT NULL,
  `conversion_name` varchar(100) DEFAULT NULL,
  `conversion_rule` varchar(300) DEFAULT NULL,
  `created_at` date DEFAULT NULL,
  KEY `conversion_id` (`conversion_id`)
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
  `score` float DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `release_default_price`
--

DROP TABLE IF EXISTS `release_default_price`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `release_default_price` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `default_price` blob,
  `request_time` datetime DEFAULT NULL
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
  `request_time` datetime DEFAULT NULL
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

-- Dump completed on 2019-09-20 11:25:11

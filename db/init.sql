-- social_media.s_xhs_data_overview_traffic_analysis definition

CREATE TABLE `s_xhs_data_overview_traffic_analysis` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `url` varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '链接',
  `title` text COLLATE utf8mb4_unicode_ci COMMENT '作品标题',
  `source_homepage` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-首页推荐',
  `source_video` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-视频推荐',
  `interaction_rate` text COLLATE utf8mb4_unicode_ci COMMENT '观看互动率',
  `completion_rate` text COLLATE utf8mb4_unicode_ci COMMENT '完播率',
  `type` text COLLATE utf8mb4_unicode_ci COMMENT '内容类型',
  `source_follow` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-关注页面',
  `source_user_homepage` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-个人主页',
  `exposure_ratio` text COLLATE utf8mb4_unicode_ci COMMENT '占总曝光',
  `account_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '账号ID',
  `followers_gained` text COLLATE utf8mb4_unicode_ci COMMENT '涨粉数',
  `shares` text COLLATE utf8mb4_unicode_ci COMMENT '分享数',
  `collection_time` datetime DEFAULT NULL COMMENT '采集时间',
  `view_count` text COLLATE utf8mb4_unicode_ci COMMENT '观看数',
  `exposure_count` text COLLATE utf8mb4_unicode_ci COMMENT '曝光数',
  `source_type` text COLLATE utf8mb4_unicode_ci COMMENT '数据来源',
  `click_rate` text COLLATE utf8mb4_unicode_ci COMMENT '封面点击率',
  `view_ratio` text COLLATE utf8mb4_unicode_ci COMMENT '占总观看',
  `conversion_rate` text COLLATE utf8mb4_unicode_ci COMMENT '观看转化率',
  `likes` text COLLATE utf8mb4_unicode_ci COMMENT '点赞数',
  `bullet` text COLLATE utf8mb4_unicode_ci COMMENT '弹幕数',
  `device_ip` text COLLATE utf8mb4_unicode_ci COMMENT '设备IP',
  `source_other` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-其他来源',
  `avg_watch_duration` text COLLATE utf8mb4_unicode_ci COMMENT '平均观看时长',
  `collects` text COLLATE utf8mb4_unicode_ci COMMENT '收藏数',
  `comments` text COLLATE utf8mb4_unicode_ci COMMENT '评论数',
  `source_search` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-搜索',
  `exit_rate_2s` text COLLATE utf8mb4_unicode_ci COMMENT '2秒退出率',
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_constraint_account_id_collection_time_url` (`account_id`,`collection_time`,`url`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



-- social_media.s_xhs_user_info_ocr definition

CREATE TABLE `s_xhs_user_info_ocr` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `source_type` text COLLATE utf8mb4_unicode_ci COMMENT '数据来源',
  `device_ip` text COLLATE utf8mb4_unicode_ci COMMENT '设备IP',
  `account_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '账号ID',
  `nickname` text COLLATE utf8mb4_unicode_ci COMMENT '账号昵称',
  `collection_time` datetime DEFAULT NULL COMMENT '采集时间',
  `follows` text COLLATE utf8mb4_unicode_ci COMMENT '关注数',
  `fans` text COLLATE utf8mb4_unicode_ci COMMENT '粉丝数',
  `interaction` text COLLATE utf8mb4_unicode_ci COMMENT '获赞与收藏',
  `url` varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '链接',
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_constraint_account_id_collection_time` (`account_id`,`collection_time`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
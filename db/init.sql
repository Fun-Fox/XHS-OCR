-- social_media.s_xhs_data_overview_traffic_analysis definition

CREATE TABLE `s_xhs_data_overview_traffic_analysis` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `click_rate` text COLLATE utf8mb4_unicode_ci COMMENT '封面点击率',
  `collects` text COLLATE utf8mb4_unicode_ci COMMENT '收藏数',
  `source_homepage` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-首页推荐',
  `source_follow` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-关注页面',
  `completion_rate` text COLLATE utf8mb4_unicode_ci COMMENT '完播率',
  `exposure_count` text COLLATE utf8mb4_unicode_ci COMMENT '曝光数',
  `title` text COLLATE utf8mb4_unicode_ci COMMENT '作品标题',
  `collection_time` date DEFAULT NULL COMMENT '采集时间',
  `followers_gained` text COLLATE utf8mb4_unicode_ci COMMENT '涨粉数',
  `avg_watch_duration` text COLLATE utf8mb4_unicode_ci COMMENT '平均观看时长',
  `url` text COLLATE utf8mb4_unicode_ci COMMENT '链接',
  `view_ratio` text COLLATE utf8mb4_unicode_ci COMMENT '占总观看',
  `interaction_rate` text COLLATE utf8mb4_unicode_ci COMMENT '观看互动率',
  `source_search` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-搜索',
  `source_user_homepage` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-个人主页',
  `shares` text COLLATE utf8mb4_unicode_ci COMMENT '分享数',
  `view_count` text COLLATE utf8mb4_unicode_ci COMMENT '观看数',
  `source_other` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-其他来源',
  `likes` text COLLATE utf8mb4_unicode_ci COMMENT '点赞数',
  `comments` text COLLATE utf8mb4_unicode_ci COMMENT '评论数',
  `bullet` text COLLATE utf8mb4_unicode_ci COMMENT '弹幕数',
  `conversion_rate` text COLLATE utf8mb4_unicode_ci COMMENT '观看转化率',
  `type` text COLLATE utf8mb4_unicode_ci COMMENT '内容类型',
  `source_type` text COLLATE utf8mb4_unicode_ci COMMENT '数据来源',
  `device_ip` text COLLATE utf8mb4_unicode_ci COMMENT '设备IP',
  `account_id` text COLLATE utf8mb4_unicode_ci COMMENT '账号ID',
  `exposure_ratio` text COLLATE utf8mb4_unicode_ci COMMENT '占总曝光',
  `source_video` text COLLATE utf8mb4_unicode_ci COMMENT '观看来源-视频推荐',
  `exit_rate_2s` text COLLATE utf8mb4_unicode_ci COMMENT '2秒退出率',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE `s_xhs_user_info_ocr` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `source_type` text COLLATE utf8mb4_unicode_ci COMMENT '数据来源',
  `device_ip` text COLLATE utf8mb4_unicode_ci COMMENT '设备IP',
  `account_id` text COLLATE utf8mb4_unicode_ci COMMENT '账号ID',
  `nickname` text COLLATE utf8mb4_unicode_ci COMMENT '账号昵称',
  `collection_date` date DEFAULT NULL COMMENT '采集日期',
  `follows` text COLLATE utf8mb4_unicode_ci COMMENT '关注数',
  `fans` text COLLATE utf8mb4_unicode_ci COMMENT '粉丝数',
  `interaction` text COLLATE utf8mb4_unicode_ci COMMENT '获赞与收藏',
  `url` text COLLATE utf8mb4_unicode_ci COMMENT '链接',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


 CREATE TABLE s_xhs_user_info_ocr ( `id` BIGINT AUTO_INCREMENT PRIMARY KEY, UNIQUE KEY `unique_constraint_account_id_collection_time` (`account_id`, `collection_time`), `source_type` TEXT COMMENT '数据来源', `device_ip` TEXT COMMENT '设备IP', `account_id` VARCHAR(255) COMMENT '账号ID', `nickname` TEXT COMMENT '账号昵称', `collection_time` DATETIME COMMENT '采集时间', `follows` TEXT COMMENT '关注数', `fans` TEXT COMMENT '粉丝数', `interaction` TEXT COMMENT '获赞与收藏', `url` VARCHAR(512) COMMENT '链接' ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci

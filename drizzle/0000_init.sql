CREATE TABLE `accounts` (
	`id` text PRIMARY KEY NOT NULL,
	`account_id` text NOT NULL,
	`provider_id` text NOT NULL,
	`user_id` text NOT NULL,
	`access_token` text,
	`refresh_token` text,
	`id_token` text,
	`access_token_expires_at` integer,
	`refresh_token_expires_at` integer,
	`scope` text,
	`password` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `accounts_user_idx` ON `accounts` (`user_id`);--> statement-breakpoint
CREATE TABLE `addresses` (
	`id` text PRIMARY KEY NOT NULL,
	`user_id` text NOT NULL,
	`label` text NOT NULL,
	`zone_id` text,
	`area` text NOT NULL,
	`street` text NOT NULL,
	`building` text,
	`floor` text,
	`notes` text,
	`lat` real,
	`lng` real,
	`is_default` integer DEFAULT false NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `addresses_user_idx` ON `addresses` (`user_id`);--> statement-breakpoint
CREATE TABLE `analytics_events` (
	`id` text PRIMARY KEY NOT NULL,
	`visitor` text NOT NULL,
	`name` text NOT NULL,
	`path` text NOT NULL,
	`locale` text,
	`referrer` text,
	`device` text,
	`meta` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `analytics_at_idx` ON `analytics_events` (`created_at`);--> statement-breakpoint
CREATE INDEX `analytics_name_idx` ON `analytics_events` (`name`,`created_at`);--> statement-breakpoint
CREATE TABLE `audit_logs` (
	`id` text PRIMARY KEY NOT NULL,
	`actor_id` text,
	`actor_email` text,
	`action` text NOT NULL,
	`entity` text NOT NULL,
	`entity_id` text,
	`summary` text,
	`diff` text,
	`ip` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `audit_logs_at_idx` ON `audit_logs` (`created_at`);--> statement-breakpoint
CREATE INDEX `audit_logs_entity_idx` ON `audit_logs` (`entity`,`entity_id`);--> statement-breakpoint
CREATE TABLE `branches` (
	`id` text PRIMARY KEY NOT NULL,
	`slug` text NOT NULL,
	`name` text NOT NULL,
	`short_name` text NOT NULL,
	`city` text NOT NULL,
	`district` text NOT NULL,
	`address` text NOT NULL,
	`story` text,
	`time_zone` text NOT NULL,
	`lat` real NOT NULL,
	`lng` real NOT NULL,
	`phone` text NOT NULL,
	`whatsapp` text,
	`email` text NOT NULL,
	`parking` text,
	`accessibility` text,
	`hero_image_id` text,
	`reservation_settings` text NOT NULL,
	`ordering_settings` text NOT NULL,
	`reservations_enabled` integer DEFAULT true NOT NULL,
	`delivery_enabled` integer DEFAULT true NOT NULL,
	`pickup_enabled` integer DEFAULT true NOT NULL,
	`dine_in_enabled` integer DEFAULT true NOT NULL,
	`ordering_paused` integer DEFAULT false NOT NULL,
	`busy_mode` integer DEFAULT false NOT NULL,
	`is_active` integer DEFAULT true NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `branches_slug_idx` ON `branches` (`slug`);--> statement-breakpoint
CREATE TABLE `carts` (
	`user_id` text PRIMARY KEY NOT NULL,
	`branch_id` text,
	`channel` text,
	`lines` text NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE TABLE `category_items` (
	`category_id` text NOT NULL,
	`item_id` text NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL,
	PRIMARY KEY(`category_id`, `item_id`),
	FOREIGN KEY (`category_id`) REFERENCES `menu_categories`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`item_id`) REFERENCES `menu_items`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `category_items_item_idx` ON `category_items` (`item_id`);--> statement-breakpoint
CREATE TABLE `content_blocks` (
	`id` text PRIMARY KEY NOT NULL,
	`page` text NOT NULL,
	`key` text NOT NULL,
	`data` text NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `content_blocks_page_key_idx` ON `content_blocks` (`page`,`key`);--> statement-breakpoint
CREATE TABLE `delivery_zones` (
	`id` text PRIMARY KEY NOT NULL,
	`branch_id` text NOT NULL,
	`name` text NOT NULL,
	`kind` text DEFAULT 'area' NOT NULL,
	`areas` text,
	`radius_km` real,
	`fee` integer NOT NULL,
	`min_order` integer NOT NULL,
	`eta_minutes` integer NOT NULL,
	`is_active` integer DEFAULT true NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `delivery_zones_branch_idx` ON `delivery_zones` (`branch_id`);--> statement-breakpoint
CREATE TABLE `dining_tables` (
	`id` text PRIMARY KEY NOT NULL,
	`branch_id` text NOT NULL,
	`code` text NOT NULL,
	`label` text NOT NULL,
	`area` text NOT NULL,
	`min_seats` integer NOT NULL,
	`max_seats` integer NOT NULL,
	`combine_group` text,
	`reservable` integer DEFAULT true NOT NULL,
	`qr_enabled` integer DEFAULT true NOT NULL,
	`is_active` integer DEFAULT true NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE UNIQUE INDEX `dining_tables_code_idx` ON `dining_tables` (`code`);--> statement-breakpoint
CREATE INDEX `dining_tables_branch_idx` ON `dining_tables` (`branch_id`);--> statement-breakpoint
CREATE TABLE `email_outbox` (
	`id` text PRIMARY KEY NOT NULL,
	`to` text NOT NULL,
	`subject` text NOT NULL,
	`html` text NOT NULL,
	`text` text NOT NULL,
	`template` text NOT NULL,
	`locale` text NOT NULL,
	`meta` text,
	`provider` text NOT NULL,
	`status` text NOT NULL,
	`error` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `email_outbox_created_idx` ON `email_outbox` (`created_at`);--> statement-breakpoint
CREATE TABLE `event_bookings` (
	`id` text PRIMARY KEY NOT NULL,
	`code` text NOT NULL,
	`event_id` text NOT NULL,
	`ticket_type_id` text NOT NULL,
	`quantity` integer NOT NULL,
	`total` integer NOT NULL,
	`name` text NOT NULL,
	`email` text NOT NULL,
	`phone` text NOT NULL,
	`locale` text DEFAULT 'ar' NOT NULL,
	`user_id` text,
	`status` text NOT NULL,
	`payment_id` text,
	`checked_in_at` integer,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`event_id`) REFERENCES `events`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`ticket_type_id`) REFERENCES `event_ticket_types`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE UNIQUE INDEX `event_bookings_code_idx` ON `event_bookings` (`code`);--> statement-breakpoint
CREATE INDEX `event_bookings_event_idx` ON `event_bookings` (`event_id`);--> statement-breakpoint
CREATE TABLE `event_ticket_types` (
	`id` text PRIMARY KEY NOT NULL,
	`event_id` text NOT NULL,
	`name` text NOT NULL,
	`description` text,
	`price` integer NOT NULL,
	`capacity` integer,
	`sort_order` integer DEFAULT 0 NOT NULL,
	FOREIGN KEY (`event_id`) REFERENCES `events`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `event_ticket_types_event_idx` ON `event_ticket_types` (`event_id`);--> statement-breakpoint
CREATE TABLE `events` (
	`id` text PRIMARY KEY NOT NULL,
	`slug` text NOT NULL,
	`kind` text NOT NULL,
	`title` text NOT NULL,
	`summary` text NOT NULL,
	`body` text,
	`branch_id` text NOT NULL,
	`starts_at` integer NOT NULL,
	`ends_at` integer NOT NULL,
	`capacity` integer NOT NULL,
	`image_id` text,
	`is_published` integer DEFAULT true NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE UNIQUE INDEX `events_slug_idx` ON `events` (`slug`);--> statement-breakpoint
CREATE INDEX `events_starts_idx` ON `events` (`starts_at`);--> statement-breakpoint
CREATE TABLE `faqs` (
	`id` text PRIMARY KEY NOT NULL,
	`category` text NOT NULL,
	`question` text NOT NULL,
	`answer` text NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL
);
--> statement-breakpoint
CREATE TABLE `favorites` (
	`user_id` text NOT NULL,
	`item_id` text NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	PRIMARY KEY(`user_id`, `item_id`),
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`item_id`) REFERENCES `menu_items`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE TABLE `files` (
	`id` text PRIMARY KEY NOT NULL,
	`key` text NOT NULL,
	`name` text NOT NULL,
	`mime` text NOT NULL,
	`size` integer NOT NULL,
	`purpose` text NOT NULL,
	`owner_id` text,
	`is_public` integer DEFAULT false NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE TABLE `gallery_items` (
	`id` text PRIMARY KEY NOT NULL,
	`media_id` text NOT NULL,
	`caption` text,
	`hour` text,
	`sort_order` integer DEFAULT 0 NOT NULL,
	FOREIGN KEY (`media_id`) REFERENCES `media`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE TABLE `gift_card_transactions` (
	`id` text PRIMARY KEY NOT NULL,
	`gift_card_id` text NOT NULL,
	`kind` text NOT NULL,
	`amount` integer NOT NULL,
	`order_id` text,
	`note` text,
	`actor_id` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`gift_card_id`) REFERENCES `gift_cards`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `gift_card_tx_card_idx` ON `gift_card_transactions` (`gift_card_id`);--> statement-breakpoint
CREATE TABLE `gift_cards` (
	`id` text PRIMARY KEY NOT NULL,
	`code` text NOT NULL,
	`initial_amount` integer NOT NULL,
	`balance` integer NOT NULL,
	`currency` text NOT NULL,
	`design` text NOT NULL,
	`purchaser_name` text NOT NULL,
	`purchaser_email` text NOT NULL,
	`recipient_name` text NOT NULL,
	`recipient_email` text NOT NULL,
	`message` text,
	`locale` text DEFAULT 'ar' NOT NULL,
	`deliver_at` integer,
	`delivered_at` integer,
	`status` text NOT NULL,
	`expires_at` integer,
	`payment_id` text,
	`user_id` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `gift_cards_code_idx` ON `gift_cards` (`code`);--> statement-breakpoint
CREATE INDEX `gift_cards_recipient_idx` ON `gift_cards` (`recipient_email`);--> statement-breakpoint
CREATE TABLE `inquiries` (
	`id` text PRIMARY KEY NOT NULL,
	`kind` text NOT NULL,
	`branch_id` text,
	`name` text NOT NULL,
	`email` text NOT NULL,
	`phone` text,
	`date` text,
	`guests` integer,
	`package_id` text,
	`message` text NOT NULL,
	`locale` text DEFAULT 'ar' NOT NULL,
	`status` text DEFAULT 'new' NOT NULL,
	`staff_notes` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `inquiries_status_idx` ON `inquiries` (`status`,`kind`);--> statement-breakpoint
CREATE TABLE `item_branches` (
	`item_id` text NOT NULL,
	`branch_id` text NOT NULL,
	`available` integer DEFAULT true NOT NULL,
	`sold_out` integer DEFAULT false NOT NULL,
	`price_override` integer,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	PRIMARY KEY(`item_id`, `branch_id`),
	FOREIGN KEY (`item_id`) REFERENCES `menu_items`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE TABLE `item_modifier_groups` (
	`item_id` text NOT NULL,
	`group_id` text NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL,
	PRIMARY KEY(`item_id`, `group_id`),
	FOREIGN KEY (`item_id`) REFERENCES `menu_items`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`group_id`) REFERENCES `modifier_groups`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE TABLE `job_applications` (
	`id` text PRIMARY KEY NOT NULL,
	`posting_id` text NOT NULL,
	`name` text NOT NULL,
	`email` text NOT NULL,
	`phone` text NOT NULL,
	`message` text,
	`cv_key` text NOT NULL,
	`cv_name` text NOT NULL,
	`status` text DEFAULT 'new' NOT NULL,
	`locale` text DEFAULT 'ar' NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`posting_id`) REFERENCES `job_postings`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `job_applications_posting_idx` ON `job_applications` (`posting_id`);--> statement-breakpoint
CREATE TABLE `job_postings` (
	`id` text PRIMARY KEY NOT NULL,
	`slug` text NOT NULL,
	`title` text NOT NULL,
	`branch_id` text,
	`employment` text NOT NULL,
	`summary` text NOT NULL,
	`description` text NOT NULL,
	`is_open` integer DEFAULT true NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `job_postings_slug_idx` ON `job_postings` (`slug`);--> statement-breakpoint
CREATE TABLE `journal_posts` (
	`id` text PRIMARY KEY NOT NULL,
	`slug` text NOT NULL,
	`kind` text NOT NULL,
	`title` text NOT NULL,
	`excerpt` text NOT NULL,
	`body` text NOT NULL,
	`cover_image_id` text,
	`author` text NOT NULL,
	`reading_minutes` integer DEFAULT 4 NOT NULL,
	`recipe` text,
	`published_at` integer,
	`is_published` integer DEFAULT true NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `journal_posts_slug_idx` ON `journal_posts` (`slug`);--> statement-breakpoint
CREATE INDEX `journal_posts_published_idx` ON `journal_posts` (`published_at`);--> statement-breakpoint
CREATE TABLE `loyalty_rewards` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`description` text,
	`points_cost` integer NOT NULL,
	`value` integer NOT NULL,
	`min_tier` text,
	`is_active` integer DEFAULT true NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL
);
--> statement-breakpoint
CREATE TABLE `loyalty_transactions` (
	`id` text PRIMARY KEY NOT NULL,
	`user_id` text NOT NULL,
	`kind` text NOT NULL,
	`points` integer NOT NULL,
	`order_id` text,
	`reward_id` text,
	`note` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `loyalty_tx_user_idx` ON `loyalty_transactions` (`user_id`);--> statement-breakpoint
CREATE TABLE `media` (
	`id` text PRIMARY KEY NOT NULL,
	`src` text NOT NULL,
	`kind` text DEFAULT 'image' NOT NULL,
	`width` integer NOT NULL,
	`height` integer NOT NULL,
	`blur_data_url` text,
	`focal_x` real DEFAULT 0.5 NOT NULL,
	`focal_y` real DEFAULT 0.5 NOT NULL,
	`alt` text NOT NULL,
	`credit` text,
	`poster` text,
	`sources` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE TABLE `menu_categories` (
	`id` text PRIMARY KEY NOT NULL,
	`menu_id` text NOT NULL,
	`slug` text NOT NULL,
	`name` text NOT NULL,
	`description` text,
	`sort_order` integer DEFAULT 0 NOT NULL,
	FOREIGN KEY (`menu_id`) REFERENCES `menus`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `menu_categories_menu_idx` ON `menu_categories` (`menu_id`);--> statement-breakpoint
CREATE TABLE `menu_items` (
	`id` text PRIMARY KEY NOT NULL,
	`slug` text NOT NULL,
	`name` text NOT NULL,
	`description` text NOT NULL,
	`story` text,
	`ingredients` text,
	`price` integer NOT NULL,
	`calories` integer,
	`spice_level` integer DEFAULT 0 NOT NULL,
	`allergens` text NOT NULL,
	`dietary` text NOT NULL,
	`image_id` text,
	`is_signature` integer DEFAULT false NOT NULL,
	`is_alcoholic` integer DEFAULT false NOT NULL,
	`orderable` integer DEFAULT true NOT NULL,
	`upsell` integer DEFAULT false NOT NULL,
	`prep_minutes` integer DEFAULT 15 NOT NULL,
	`pairings` text,
	`tags` text,
	`is_active` integer DEFAULT true NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `menu_items_slug_idx` ON `menu_items` (`slug`);--> statement-breakpoint
CREATE TABLE `menus` (
	`id` text PRIMARY KEY NOT NULL,
	`slug` text NOT NULL,
	`kind` text NOT NULL,
	`name` text NOT NULL,
	`hour` text,
	`description` text,
	`schedule` text NOT NULL,
	`branch_ids` text,
	`seasonal_mode_id` text,
	`is_tasting` integer DEFAULT false NOT NULL,
	`tasting_price` integer,
	`image_id` text,
	`is_active` integer DEFAULT true NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `menus_slug_idx` ON `menus` (`slug`);--> statement-breakpoint
CREATE TABLE `modifier_groups` (
	`id` text PRIMARY KEY NOT NULL,
	`key` text NOT NULL,
	`name` text NOT NULL,
	`min_select` integer DEFAULT 0 NOT NULL,
	`max_select` integer DEFAULT 1 NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL
);
--> statement-breakpoint
CREATE TABLE `modifier_options` (
	`id` text PRIMARY KEY NOT NULL,
	`group_id` text NOT NULL,
	`name` text NOT NULL,
	`price_delta` integer DEFAULT 0 NOT NULL,
	`is_default` integer DEFAULT false NOT NULL,
	`is_available` integer DEFAULT true NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL,
	FOREIGN KEY (`group_id`) REFERENCES `modifier_groups`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `modifier_options_group_idx` ON `modifier_options` (`group_id`);--> statement-breakpoint
CREATE TABLE `newsletter_subscribers` (
	`id` text PRIMARY KEY NOT NULL,
	`email` text NOT NULL,
	`locale` text DEFAULT 'ar' NOT NULL,
	`status` text DEFAULT 'pending' NOT NULL,
	`token_hash` text NOT NULL,
	`source` text,
	`confirmed_at` integer,
	`unsubscribed_at` integer,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `newsletter_email_idx` ON `newsletter_subscribers` (`email`);--> statement-breakpoint
CREATE TABLE `notifications` (
	`id` text PRIMARY KEY NOT NULL,
	`role` text,
	`user_id` text,
	`branch_id` text,
	`kind` text NOT NULL,
	`title` text NOT NULL,
	`body` text,
	`href` text,
	`read_by` text NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `notifications_created_idx` ON `notifications` (`created_at`);--> statement-breakpoint
CREATE TABLE `opening_hours` (
	`id` text PRIMARY KEY NOT NULL,
	`branch_id` text NOT NULL,
	`kind` text DEFAULT 'venue' NOT NULL,
	`weekday` integer NOT NULL,
	`opens` text NOT NULL,
	`closes` text NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `opening_hours_branch_idx` ON `opening_hours` (`branch_id`,`kind`,`weekday`);--> statement-breakpoint
CREATE TABLE `order_events` (
	`id` text PRIMARY KEY NOT NULL,
	`order_id` text NOT NULL,
	`status` text NOT NULL,
	`note` text,
	`actor_id` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`order_id`) REFERENCES `orders`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `order_events_order_idx` ON `order_events` (`order_id`);--> statement-breakpoint
CREATE TABLE `order_items` (
	`id` text PRIMARY KEY NOT NULL,
	`order_id` text NOT NULL,
	`item_id` text,
	`name` text NOT NULL,
	`unit_price` integer NOT NULL,
	`quantity` integer NOT NULL,
	`modifiers` text NOT NULL,
	`notes` text,
	`line_total` integer NOT NULL,
	FOREIGN KEY (`order_id`) REFERENCES `orders`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`item_id`) REFERENCES `menu_items`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE INDEX `order_items_order_idx` ON `order_items` (`order_id`);--> statement-breakpoint
CREATE INDEX `order_items_item_idx` ON `order_items` (`item_id`);--> statement-breakpoint
CREATE TABLE `orders` (
	`id` text PRIMARY KEY NOT NULL,
	`number` text NOT NULL,
	`token_hash` text NOT NULL,
	`branch_id` text NOT NULL,
	`user_id` text,
	`channel` text NOT NULL,
	`table_id` text,
	`status` text DEFAULT 'placed' NOT NULL,
	`asap` integer DEFAULT true NOT NULL,
	`scheduled_for` integer,
	`promised_at` integer,
	`prep_minutes` integer,
	`name` text NOT NULL,
	`email` text NOT NULL,
	`phone` text NOT NULL,
	`address` text,
	`zone_id` text,
	`notes` text,
	`locale` text DEFAULT 'ar' NOT NULL,
	`subtotal` integer NOT NULL,
	`discount` integer DEFAULT 0 NOT NULL,
	`promo_code` text,
	`delivery_fee` integer DEFAULT 0 NOT NULL,
	`service_charge` integer DEFAULT 0 NOT NULL,
	`tax` integer DEFAULT 0 NOT NULL,
	`tip` integer DEFAULT 0 NOT NULL,
	`gift_card_amount` integer DEFAULT 0 NOT NULL,
	`gift_card_id` text,
	`loyalty_points_redeemed` integer DEFAULT 0 NOT NULL,
	`loyalty_discount` integer DEFAULT 0 NOT NULL,
	`loyalty_points_earned` integer DEFAULT 0 NOT NULL,
	`total` integer NOT NULL,
	`payment_method` text NOT NULL,
	`payment_status` text DEFAULT 'pending' NOT NULL,
	`payment_id` text,
	`reject_reason` text,
	`accepted_at` integer,
	`ready_at` integer,
	`completed_at` integer,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE no action,
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE UNIQUE INDEX `orders_number_idx` ON `orders` (`number`);--> statement-breakpoint
CREATE INDEX `orders_branch_status_idx` ON `orders` (`branch_id`,`status`);--> statement-breakpoint
CREATE INDEX `orders_created_idx` ON `orders` (`created_at`);--> statement-breakpoint
CREATE INDEX `orders_user_idx` ON `orders` (`user_id`);--> statement-breakpoint
CREATE INDEX `orders_email_idx` ON `orders` (`email`);--> statement-breakpoint
CREATE TABLE `payments` (
	`id` text PRIMARY KEY NOT NULL,
	`provider` text NOT NULL,
	`purpose` text NOT NULL,
	`reference_id` text NOT NULL,
	`amount` integer NOT NULL,
	`currency` text NOT NULL,
	`status` text NOT NULL,
	`provider_ref` text,
	`client_secret` text,
	`failure_reason` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `payments_reference_idx` ON `payments` (`purpose`,`reference_id`);--> statement-breakpoint
CREATE INDEX `payments_provider_ref_idx` ON `payments` (`provider_ref`);--> statement-breakpoint
CREATE TABLE `press_items` (
	`id` text PRIMARY KEY NOT NULL,
	`kind` text NOT NULL,
	`publication` text NOT NULL,
	`title` text NOT NULL,
	`quote` text NOT NULL,
	`year` integer NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL
);
--> statement-breakpoint
CREATE TABLE `private_packages` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`description` text NOT NULL,
	`price_per_guest` integer NOT NULL,
	`min_guests` integer NOT NULL,
	`kind` text DEFAULT 'private_dining' NOT NULL,
	`sort_order` integer DEFAULT 0 NOT NULL
);
--> statement-breakpoint
CREATE TABLE `private_rooms` (
	`id` text PRIMARY KEY NOT NULL,
	`slug` text NOT NULL,
	`branch_id` text NOT NULL,
	`name` text NOT NULL,
	`description` text NOT NULL,
	`seated` integer NOT NULL,
	`standing` integer,
	`features` text,
	`image_id` text,
	`sort_order` integer DEFAULT 0 NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE TABLE `promotion_redemptions` (
	`id` text PRIMARY KEY NOT NULL,
	`promotion_id` text NOT NULL,
	`order_id` text NOT NULL,
	`email` text NOT NULL,
	`amount` integer NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`promotion_id`) REFERENCES `promotions`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `promotion_redemptions_promo_idx` ON `promotion_redemptions` (`promotion_id`,`email`);--> statement-breakpoint
CREATE TABLE `promotions` (
	`id` text PRIMARY KEY NOT NULL,
	`code` text NOT NULL,
	`description` text NOT NULL,
	`kind` text NOT NULL,
	`value` integer NOT NULL,
	`max_discount` integer,
	`min_order` integer DEFAULT 0 NOT NULL,
	`starts_at` integer,
	`ends_at` integer,
	`usage_limit` integer,
	`per_customer_limit` integer,
	`used_count` integer DEFAULT 0 NOT NULL,
	`first_order_only` integer DEFAULT false NOT NULL,
	`branch_ids` text,
	`channels` text,
	`is_active` integer DEFAULT true NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `promotions_code_idx` ON `promotions` (`code`);--> statement-breakpoint
CREATE TABLE `rate_limits` (
	`key` text PRIMARY KEY NOT NULL,
	`count` integer NOT NULL,
	`reset_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `reservation_holds` (
	`id` text PRIMARY KEY NOT NULL,
	`branch_id` text NOT NULL,
	`date` text NOT NULL,
	`time` text NOT NULL,
	`starts_at` integer NOT NULL,
	`ends_at` integer NOT NULL,
	`party_size` integer NOT NULL,
	`table_ids` text NOT NULL,
	`area` text DEFAULT 'any' NOT NULL,
	`session_key` text NOT NULL,
	`expires_at` integer NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `reservation_holds_branch_idx` ON `reservation_holds` (`branch_id`,`date`);--> statement-breakpoint
CREATE INDEX `reservation_holds_expires_idx` ON `reservation_holds` (`expires_at`);--> statement-breakpoint
CREATE TABLE `reservations` (
	`id` text PRIMARY KEY NOT NULL,
	`code` text NOT NULL,
	`token_hash` text NOT NULL,
	`branch_id` text NOT NULL,
	`user_id` text,
	`date` text NOT NULL,
	`time` text NOT NULL,
	`starts_at` integer NOT NULL,
	`ends_at` integer NOT NULL,
	`party_size` integer NOT NULL,
	`area` text DEFAULT 'any' NOT NULL,
	`occasion` text,
	`table_ids` text NOT NULL,
	`status` text DEFAULT 'confirmed' NOT NULL,
	`name` text NOT NULL,
	`email` text NOT NULL,
	`phone` text NOT NULL,
	`notes` text,
	`dietary_notes` text,
	`staff_notes` text,
	`locale` text DEFAULT 'ar' NOT NULL,
	`source` text DEFAULT 'web' NOT NULL,
	`experience_id` text,
	`deposit_amount` integer DEFAULT 0 NOT NULL,
	`deposit_status` text DEFAULT 'none' NOT NULL,
	`payment_id` text,
	`reminder_sent_at` integer,
	`seated_at` integer,
	`completed_at` integer,
	`cancelled_at` integer,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE no action,
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE set null
);
--> statement-breakpoint
CREATE UNIQUE INDEX `reservations_code_idx` ON `reservations` (`code`);--> statement-breakpoint
CREATE INDEX `reservations_branch_date_idx` ON `reservations` (`branch_id`,`date`);--> statement-breakpoint
CREATE INDEX `reservations_starts_idx` ON `reservations` (`starts_at`);--> statement-breakpoint
CREATE INDEX `reservations_user_idx` ON `reservations` (`user_id`);--> statement-breakpoint
CREATE INDEX `reservations_email_idx` ON `reservations` (`email`);--> statement-breakpoint
CREATE TABLE `reviews` (
	`id` text PRIMARY KEY NOT NULL,
	`branch_id` text,
	`user_id` text,
	`name` text NOT NULL,
	`email` text,
	`rating` integer NOT NULL,
	`title` text,
	`body` text NOT NULL,
	`locale` text NOT NULL,
	`visit_date` text,
	`status` text DEFAULT 'pending' NOT NULL,
	`response` text,
	`featured` integer DEFAULT false NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `reviews_status_idx` ON `reviews` (`status`);--> statement-breakpoint
CREATE TABLE `seasonal_modes` (
	`id` text PRIMARY KEY NOT NULL,
	`slug` text NOT NULL,
	`kind` text NOT NULL,
	`name` text NOT NULL,
	`banner` text,
	`start_date` text NOT NULL,
	`end_date` text NOT NULL,
	`theme` text DEFAULT 'none' NOT NULL,
	`hours` text,
	`is_enabled` integer DEFAULT true NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `seasonal_modes_slug_idx` ON `seasonal_modes` (`slug`);--> statement-breakpoint
CREATE TABLE `service_periods` (
	`id` text PRIMARY KEY NOT NULL,
	`branch_id` text NOT NULL,
	`key` text NOT NULL,
	`name` text NOT NULL,
	`weekdays` text NOT NULL,
	`start` text NOT NULL,
	`end` text NOT NULL,
	`max_covers_per_slot` integer,
	`sort_order` integer DEFAULT 0 NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `service_periods_branch_idx` ON `service_periods` (`branch_id`);--> statement-breakpoint
CREATE TABLE `sessions` (
	`id` text PRIMARY KEY NOT NULL,
	`expires_at` integer NOT NULL,
	`token` text NOT NULL,
	`ip_address` text,
	`user_agent` text,
	`user_id` text NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE UNIQUE INDEX `sessions_token_idx` ON `sessions` (`token`);--> statement-breakpoint
CREATE INDEX `sessions_user_idx` ON `sessions` (`user_id`);--> statement-breakpoint
CREATE TABLE `settings` (
	`key` text PRIMARY KEY NOT NULL,
	`value` text NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE TABLE `special_hours` (
	`id` text PRIMARY KEY NOT NULL,
	`branch_id` text NOT NULL,
	`date` text NOT NULL,
	`label` text NOT NULL,
	`closed` integer DEFAULT false NOT NULL,
	`ranges` text,
	`reservations_blocked` integer DEFAULT false NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE UNIQUE INDEX `special_hours_branch_date_idx` ON `special_hours` (`branch_id`,`date`);--> statement-breakpoint
CREATE TABLE `table_requests` (
	`id` text PRIMARY KEY NOT NULL,
	`branch_id` text NOT NULL,
	`table_id` text NOT NULL,
	`kind` text NOT NULL,
	`note` text,
	`status` text DEFAULT 'open' NOT NULL,
	`acknowledged_by` text,
	`acknowledged_at` integer,
	`done_at` integer,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`table_id`) REFERENCES `dining_tables`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `table_requests_branch_status_idx` ON `table_requests` (`branch_id`,`status`);--> statement-breakpoint
CREATE TABLE `team_members` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`role` text NOT NULL,
	`bio` text NOT NULL,
	`image_id` text,
	`branch_id` text,
	`sort_order` integer DEFAULT 0 NOT NULL
);
--> statement-breakpoint
CREATE TABLE `users` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`email` text NOT NULL,
	`email_verified` integer DEFAULT false NOT NULL,
	`image` text,
	`role` text DEFAULT 'customer' NOT NULL,
	`phone` text,
	`locale` text DEFAULT 'ar' NOT NULL,
	`branch_id` text,
	`dietary` text,
	`marketing_email` integer DEFAULT false NOT NULL,
	`marketing_sms` integer DEFAULT false NOT NULL,
	`loyalty_points` integer DEFAULT 0 NOT NULL,
	`lifetime_points` integer DEFAULT 0 NOT NULL,
	`tags` text,
	`staff_notes` text,
	`disabled` integer DEFAULT false NOT NULL,
	`deleted_at` integer,
	`last_seen_at` integer,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `users_email_idx` ON `users` (`email`);--> statement-breakpoint
CREATE INDEX `users_role_idx` ON `users` (`role`);--> statement-breakpoint
CREATE TABLE `verifications` (
	`id` text PRIMARY KEY NOT NULL,
	`identifier` text NOT NULL,
	`value` text NOT NULL,
	`expires_at` integer NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL
);
--> statement-breakpoint
CREATE INDEX `verifications_identifier_idx` ON `verifications` (`identifier`);--> statement-breakpoint
CREATE TABLE `waitlist_entries` (
	`id` text PRIMARY KEY NOT NULL,
	`branch_id` text NOT NULL,
	`date` text NOT NULL,
	`preferred_time` text NOT NULL,
	`flexible_minutes` integer DEFAULT 60 NOT NULL,
	`party_size` integer NOT NULL,
	`name` text NOT NULL,
	`email` text NOT NULL,
	`phone` text NOT NULL,
	`notes` text,
	`locale` text DEFAULT 'ar' NOT NULL,
	`status` text DEFAULT 'waiting' NOT NULL,
	`reservation_id` text,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`branch_id`) REFERENCES `branches`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `waitlist_branch_date_idx` ON `waitlist_entries` (`branch_id`,`date`);
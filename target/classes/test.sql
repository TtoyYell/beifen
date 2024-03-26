INSERT INTO "public"."trad_edition"
("created_at", "dispatch_status", "deal_style", "ignored", "deal_at",
"assigned_by", "accepted_by", "assigned_at", "assigned_deadline", "deleted", "reviewed", "reviewed_by", "reviewed_at",
"supervise", "deal_style2", "name", "size", "category_id", "type", "advertiser_id", "channel_id", "evidence",
 "level","banner_id", "laws", "description", "illegal_description", "created_by", "original_edition_id", "is_updated",
"first_illegal_time", "view_count", "source_id", "start_page", "end_page", "extend_phone", "tag_ids", "sp_tag_ids",
"assignment_to", "allow_distribute", "reviewed_sp_tag", "production_time", "merge_id", "reviewed_back", "timeout",
"is_timeout", "is_delay", "action_status", "user_deadline")
VALUES ('2023-04-13 03:02:01.719764+08', 0, 0, 'f', NULL,
NULL, NULL, NULL, NULL, 'f', 't', 1852, '2023-04-18 18:13:39.334027+08',
'f', NULL, '我有奇方馆 1740', '1740', 158, 2, 1, 3213,
'["b61c3e60-117a-11ed-abac-85b8ea1b6f69.1681326130459.W47772.mp3"]', 0, NULL, '[]', '', '', 1899, 47772, 0,
'2023-03-15 14:30:28+08', NULL, 'b61c3e60-117a-11ed-abac-85b8ea1b6f69', NULL, NULL, NULL, NULL, '{25}',
33,'f', 't', '2023-03-15 14:30:28+08', NULL, NULL, NULL,
'f', NULL, 0, 'f');


INSERT INTO "public"."trad_illegal"("created_at", "edition_id",
"length", "position", "illegal_at", "channel_id",
"created_by", "amount", "original_illegal_id", "type", "source_id") VALUES
('2023-04-14 11:07:18.601264+08', 109015,
 1740, NULL, '2023-03-15 14:30:28+08', 3213,
 2535, 1, NULL, 2, NULL);

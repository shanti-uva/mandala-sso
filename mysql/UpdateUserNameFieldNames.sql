select * from field_config_instance where bundle='user';
select * from field_config where field_name = 'field_first_name' or field_name = 'field_last_name';
/*
update field_config set field_name = 'field_first_name' where id=59;
update field_config set field_name = 'field_last_name' where id=60;
update field_config_instance set field_name = 'field_first_name' where id=93;
update field_config_instance set field_name = 'field_last_name' where id=94;
*/
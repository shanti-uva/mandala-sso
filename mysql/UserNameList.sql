use shanti_predev;
# select fn.field_first_name_value, ln.field_last_name_value from field_data_field_last_name ln join field_data_field_first_name fn on fn.entity_id = ln.entity_id;
select count(*) from field_data_field_last_name;
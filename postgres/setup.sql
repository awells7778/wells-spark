create schema sample_landing;
drop table if exists sample_landing.us_500;
create table sample_landing.us_500
(
    first_name     varchar(64),
    last_name      varchar(64),
    company_name   varchar(256),
    address        varchar(512),
    city           varchar(64),
    county         varchar(64),
    state          varchar(16),
    zip            varchar(16),
    phone1         varchar(32),
    phone2         varchar(32),
    email          varchar(128),
    web            varchar(512),
    load_timestamp timestamp not null
);

create schema spark_log;
create table spark_log.spark_apps_execution
(
    log_id               serial primary key,
    job_run_id           integer,
    app_name             varchar(64),
    job_action_timestamp timestamp,
    job_action_type      varchar(16)
);
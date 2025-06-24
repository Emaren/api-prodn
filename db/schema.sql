--
-- PostgreSQL database dump
--

-- Dumped from database version 17.4 (Homebrew)
-- Dumped by pg_dump version 17.4 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

ALTER TABLE ONLY public.game_stats DROP CONSTRAINT game_stats_user_uid_fkey;
DROP INDEX public.ix_users_email;
DROP INDEX public.ix_replay_iteration;
DROP INDEX public.ix_replay_hash_iteration;
DROP INDEX public.ix_game_stats_user_uid;
ALTER TABLE ONLY public.users DROP CONSTRAINT users_uid_key;
ALTER TABLE ONLY public.users DROP CONSTRAINT users_pkey;
ALTER TABLE ONLY public.game_stats DROP CONSTRAINT uq_replay_final;
ALTER TABLE ONLY public.game_stats DROP CONSTRAINT game_stats_pkey;
ALTER TABLE ONLY public.alembic_version DROP CONSTRAINT alembic_version_pkc;
ALTER TABLE public.users ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.game_stats ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE public.users_id_seq;
DROP TABLE public.users;
DROP SEQUENCE public.game_stats_id_seq;
DROP TABLE public.game_stats;
DROP TABLE public.alembic_version;
SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: aoe2user
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


ALTER TABLE public.alembic_version OWNER TO aoe2user;

--
-- Name: game_stats; Type: TABLE; Schema: public; Owner: aoe2user
--

CREATE TABLE public.game_stats (
    id integer NOT NULL,
    replay_file character varying(500) NOT NULL,
    replay_hash character varying(64) NOT NULL,
    created_at timestamp without time zone DEFAULT now(),
    game_version character varying(50),
    map character varying(100),
    game_type character varying(50),
    duration integer,
    game_duration integer,
    winner character varying(100),
    players json,
    event_types json,
    key_events json,
    "timestamp" timestamp without time zone,
    played_on timestamp without time zone,
    parse_iteration integer,
    is_final boolean,
    disconnect_detected boolean,
    parse_source character varying(20),
    parse_reason character varying(50),
    original_filename character varying(255),
    user_uid character varying
);


ALTER TABLE public.game_stats OWNER TO aoe2user;

--
-- Name: game_stats_id_seq; Type: SEQUENCE; Schema: public; Owner: aoe2user
--

CREATE SEQUENCE public.game_stats_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.game_stats_id_seq OWNER TO aoe2user;

--
-- Name: game_stats_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: aoe2user
--

ALTER SEQUENCE public.game_stats_id_seq OWNED BY public.game_stats.id;


--
-- Name: users; Type: TABLE; Schema: public; Owner: aoe2user
--

CREATE TABLE public.users (
    id integer NOT NULL,
    uid character varying NOT NULL,
    email character varying,
    in_game_name character varying,
    verified boolean,
    wallet_address character varying(100),
    lock_name boolean,
    created_at timestamp without time zone,
    token character varying(128)
);


ALTER TABLE public.users OWNER TO aoe2user;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: aoe2user
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.users_id_seq OWNER TO aoe2user;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: aoe2user
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: game_stats id; Type: DEFAULT; Schema: public; Owner: aoe2user
--

ALTER TABLE ONLY public.game_stats ALTER COLUMN id SET DEFAULT nextval('public.game_stats_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: aoe2user
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: aoe2user
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: game_stats game_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: aoe2user
--

ALTER TABLE ONLY public.game_stats
    ADD CONSTRAINT game_stats_pkey PRIMARY KEY (id);


--
-- Name: game_stats uq_replay_final; Type: CONSTRAINT; Schema: public; Owner: aoe2user
--

ALTER TABLE ONLY public.game_stats
    ADD CONSTRAINT uq_replay_final UNIQUE (replay_hash, is_final);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: aoe2user
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_uid_key; Type: CONSTRAINT; Schema: public; Owner: aoe2user
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_uid_key UNIQUE (uid);


--
-- Name: ix_game_stats_user_uid; Type: INDEX; Schema: public; Owner: aoe2user
--

CREATE INDEX ix_game_stats_user_uid ON public.game_stats USING btree (user_uid);


--
-- Name: ix_replay_hash_iteration; Type: INDEX; Schema: public; Owner: aoe2user
--

CREATE INDEX ix_replay_hash_iteration ON public.game_stats USING btree (replay_hash, parse_iteration);


--
-- Name: ix_replay_iteration; Type: INDEX; Schema: public; Owner: aoe2user
--

CREATE INDEX ix_replay_iteration ON public.game_stats USING btree (replay_file, parse_iteration);


--
-- Name: ix_users_email; Type: INDEX; Schema: public; Owner: aoe2user
--

CREATE UNIQUE INDEX ix_users_email ON public.users USING btree (email);


--
-- Name: game_stats game_stats_user_uid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: aoe2user
--

ALTER TABLE ONLY public.game_stats
    ADD CONSTRAINT game_stats_user_uid_fkey FOREIGN KEY (user_uid) REFERENCES public.users(uid);


--
-- PostgreSQL database dump complete
--


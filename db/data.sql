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

--
-- Data for Name: alembic_version; Type: TABLE DATA; Schema: public; Owner: aoe2user
--

COPY public.alembic_version (version_num) FROM stdin;
8060c36ed772
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: aoe2user
--

COPY public.users (id, uid, email, in_game_name, verified, wallet_address, lock_name, created_at, token) FROM stdin;
1	test-uid	test@example.com	TestPlayer	f	\N	f	2025-05-19 01:26:34.068191	\N
2	C00QUqpOj1aa4HwCINgPC96dz0Y2	tony.c8ccdb@aoe2hdbets.com		f	\N	f	2025-05-19 01:50:09.746593	\N
3	FYWynYLY5qeBr3PubFAN9Rgvutz1	emaren.9c4e1f@aoe2hdbets.com		f	\N	f	2025-05-19 01:50:09.866882	\N
4	VImcy265OzX2wv9js6RK5zI3yYD3	bola.92b50e@aoe2hdbets.com		f	\N	f	2025-05-19 01:50:09.868014	\N
5	VjwrFIafbcY1cKEcTWx6Hakm2hJ3	verc.efe898@aoe2hdbets.com		f	\N	f	2025-05-19 01:50:09.868846	\N
6	f8CtZAzvEAPh98yCUhFKMuemaXf2	local.c95131@aoe2hdbets.com		f	\N	f	2025-05-19 01:50:09.8697	\N
7	m33Z9jbk3BNDS94LHesCEPfrFp52	rend.f015c7@aoe2hdbets.com		f	\N	f	2025-05-19 01:50:09.870569	\N
8	vNbKKBCLL9Q7AYw1OhyCzvnaOWs2	local.fa55b6@aoe2hdbets.com		f	\N	f	2025-05-19 01:50:09.871309	\N
9	a4e5c251-39c9-4ca9-8e53-154bd75b20e3	unknown@aoe2hdbets.com	Chrome	f	\N	f	2025-05-19 02:46:10.308087	\N
\.


--
-- Data for Name: game_stats; Type: TABLE DATA; Schema: public; Owner: aoe2user
--

COPY public.game_stats (id, replay_file, replay_hash, created_at, game_version, map, game_type, duration, game_duration, winner, players, event_types, key_events, "timestamp", played_on, parse_iteration, is_final, disconnect_detected, parse_source, parse_reason, original_filename, user_uid) FROM stdin;
\.


--
-- Name: game_stats_id_seq; Type: SEQUENCE SET; Schema: public; Owner: aoe2user
--

SELECT pg_catalog.setval('public.game_stats_id_seq', 1, false);


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: aoe2user
--

SELECT pg_catalog.setval('public.users_id_seq', 9, true);


--
-- PostgreSQL database dump complete
--


import unittest
from pathlib import Path

from mgz.fast.header import parse
from mgz.util import Version

RECS_DIR = Path(__file__).resolve().parent / "recs"


def parse_fixture_or_skip(fixture_name: str):
    fixture_path = RECS_DIR / fixture_name
    if not fixture_path.exists():
        raise unittest.SkipTest(f"Missing replay fixture: {fixture_path}")

    with fixture_path.open("rb") as handle:
        return parse(handle)


class TestFastUserPatch15(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = parse_fixture_or_skip("small.mgz")

    def test_version(self):
        self.assertEqual(self.data['version'], Version.USERPATCH15)

    def test_players(self):
        players = self.data.get('players')
        self.assertEqual(len(players), 3)
        self.assertEqual(players[0]['diplomacy'], [1, 4, 4, -1, -1, -1, -1, -1, -1])
        self.assertEqual(players[1]['diplomacy'], [0, 1, 4, -1, -1, -1, -1, -1, -1])
        self.assertEqual(players[2]['diplomacy'], [0, 4, 1, -1, -1, -1, -1, -1, -1])

    def test_map(self):
        self.assertEqual(self.data['scenario']['map_id'], 44)


class TestFastDE(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = parse_fixture_or_skip("de-13.34.aoe2record")

    def test_version(self):
        self.assertEqual(self.data['version'], Version.DE)

    def test_players(self):
        players = self.data.get('players')
        self.assertEqual(len(players), 3)

    def test_map(self):
        self.assertEqual(self.data['scenario']['map_id'], 9)
        self.assertEqual(self.data['lobby']['seed'], -1970180596)


class TestFastDEScenario(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = parse_fixture_or_skip("de-50.6-scenario.aoe2record")

    def test_version(self):
        self.assertEqual(self.data['version'], Version.DE)

    def test_players(self):
        players = self.data.get('players')
        self.assertEqual(len(players), 3)


class TestFastDEScenarioWithTriggers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = parse_fixture_or_skip("de-50.6-scenario-with-triggers.aoe2record")

    def test_version(self):
        self.assertEqual(self.data['version'], Version.DE)

    def test_players(self):
        players = self.data.get('players')
        self.assertEqual(len(players), 3)


class TestFastHD(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = parse_fixture_or_skip("hd-5.8.aoe2record")

    def test_version(self):
        self.assertEqual(self.data['version'], Version.HD)

    def test_players(self):
        players = self.data.get('players')
        self.assertEqual(len(players), 7)

    def test_map(self):
        self.assertEqual(self.data['scenario']['map_id'], 0)

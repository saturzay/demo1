import pytest
from django.contrib.gis.geos import Point

from core.models import PointOfInterest


@pytest.mark.django_db
def test_point_of_interest_round_trip():
    poi = PointOfInterest.objects.create(name="Test Point", location=Point(-122.42, 37.77))

    fetched = PointOfInterest.objects.get(id=poi.id)

    assert fetched.name == "Test Point"
    assert fetched.location.srid == 4326
    assert round(fetched.location.x, 2) == -122.42
    assert round(fetched.location.y, 2) == 37.77

import pytest
from django.contrib.gis.geos import Point

from drillholes.models import MrdsSite, WaDrillhole


@pytest.mark.django_db
def test_wa_drillhole_round_trip():
    hole = WaDrillhole.objects.create(
        collarid=117597,
        hole_id="FCRB0083",
        location=Point(120.65832572137369, -28.04357053765508),
        latitude=-28.04357053765508,
        longitude=120.65832572137369,
        target_commodity="GOLD",
        max_depth=38.0,
    )

    fetched = WaDrillhole.objects.get(collarid=117597)

    assert fetched.hole_id == "FCRB0083"
    assert fetched.location.srid == 4326
    assert round(fetched.location.x, 2) == round(hole.location.x, 2)


@pytest.mark.django_db
def test_mrds_site_round_trip():
    site = MrdsSite.objects.create(
        dep_id="10009306",
        site_name="Horse Mountain",
        location=Point(-106.20061, 39.69999),
        latitude=39.69999,
        longitude=-106.20061,
        country="United States",
        commodity="Uranium",
    )

    fetched = MrdsSite.objects.get(dep_id="10009306")

    assert fetched.site_name == "Horse Mountain"
    assert fetched.location.srid == 4326
    assert round(fetched.location.y, 2) == round(site.location.y, 2)

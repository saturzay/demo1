from django.contrib.gis.db import models


class WaDrillhole(models.Model):
    """A drillhole collar from the WA DMIRS open-file company drillhole
    database (DMIRS-046), sourced from the SLIP ArcGIS REST service."""

    collarid = models.IntegerField(unique=True)
    hole_id = models.CharField(max_length=100, blank=True, null=True)
    location = models.PointField(geography=True, null=True)
    latitude = models.FloatField(null=True)
    longitude = models.FloatField(null=True)
    target_commodity = models.CharField(max_length=255, blank=True, null=True)
    max_depth = models.FloatField(null=True)
    operator = models.CharField(max_length=255, blank=True, null=True)
    project = models.CharField(max_length=255, blank=True, null=True)
    hole_type = models.CharField(max_length=50, blank=True, null=True)
    anumber = models.IntegerField(null=True)
    period_from = models.DateField(null=True)
    period_to = models.DateField(null=True)
    extract_date = models.DateField(null=True)

    def __str__(self):
        return self.hole_id or str(self.collarid)


class MrdsSite(models.Model):
    """A mineral occurrence/deposit from the USGS Mineral Resources Data
    System (MRDS)."""

    dep_id = models.CharField(max_length=50, unique=True)
    site_name = models.CharField(max_length=255, blank=True, null=True)
    location = models.PointField(geography=True, null=True)
    latitude = models.FloatField(null=True)
    longitude = models.FloatField(null=True)
    country = models.CharField(max_length=100, blank=True, null=True)
    state = models.CharField(max_length=100, blank=True, null=True)
    commodity = models.CharField(max_length=255, blank=True, null=True)
    development_status = models.CharField(max_length=100, blank=True, null=True)
    deposit_type = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.site_name or self.dep_id

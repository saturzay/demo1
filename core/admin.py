from django.contrib.gis import admin

from .models import PointOfInterest

admin.site.register(PointOfInterest, admin.GISModelAdmin)

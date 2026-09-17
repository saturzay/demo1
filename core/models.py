from django.contrib.gis.db import models


class PointOfInterest(models.Model):
    name = models.CharField(max_length=255)
    location = models.PointField(geography=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name

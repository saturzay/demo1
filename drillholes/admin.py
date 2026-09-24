from django.contrib.gis import admin

from .models import MrdsSite, WaDrillhole

admin.site.register(WaDrillhole, admin.GISModelAdmin)
admin.site.register(MrdsSite, admin.GISModelAdmin)

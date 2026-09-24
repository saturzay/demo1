import django.contrib.gis.db.models.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="WaDrillhole",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("collarid", models.IntegerField(unique=True)),
                ("hole_id", models.CharField(blank=True, max_length=100, null=True)),
                ("location", django.contrib.gis.db.models.fields.PointField(geography=True, null=True, srid=4326)),
                ("latitude", models.FloatField(null=True)),
                ("longitude", models.FloatField(null=True)),
                ("target_commodity", models.CharField(blank=True, max_length=255, null=True)),
                ("max_depth", models.FloatField(null=True)),
                ("operator", models.CharField(blank=True, max_length=255, null=True)),
                ("project", models.CharField(blank=True, max_length=255, null=True)),
                ("hole_type", models.CharField(blank=True, max_length=50, null=True)),
                ("anumber", models.IntegerField(null=True)),
                ("period_from", models.DateField(null=True)),
                ("period_to", models.DateField(null=True)),
                ("extract_date", models.DateField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name="MrdsSite",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("dep_id", models.CharField(max_length=50, unique=True)),
                ("site_name", models.CharField(blank=True, max_length=255, null=True)),
                ("location", django.contrib.gis.db.models.fields.PointField(geography=True, null=True, srid=4326)),
                ("latitude", models.FloatField(null=True)),
                ("longitude", models.FloatField(null=True)),
                ("country", models.CharField(blank=True, max_length=100, null=True)),
                ("state", models.CharField(blank=True, max_length=100, null=True)),
                ("commodity", models.CharField(blank=True, max_length=255, null=True)),
                ("development_status", models.CharField(blank=True, max_length=100, null=True)),
                ("deposit_type", models.CharField(blank=True, max_length=255, null=True)),
            ],
        ),
    ]

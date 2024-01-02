# Generated by Django 5.0 on 2023-12-19 23:50

import django.contrib.gis.db.models.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CityLocation',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('city', models.CharField(max_length=32)),
                ('country', models.CharField(max_length=32)),
                ('population', models.PositiveIntegerField(default=0)),
                ('city_location', django.contrib.gis.db.models.fields.PointField(srid=4326)),
            ],
        ),
        migrations.CreateModel(
            name='Station',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('station_id', models.CharField(max_length=11, unique=True)),
                ('elevation', models.DecimalField(decimal_places=1, max_digits=6)),
                ('station_state', models.CharField(blank=True, max_length=2)),
                ('station_name', models.CharField(max_length=30)),
                ('gsn_flag', models.BooleanField(default=False)),
                ('wmo_id', models.PositiveIntegerField(default=0)),
                ('station_location', django.contrib.gis.db.models.fields.PointField(srid=4326)),
            ],
        ),
    ]

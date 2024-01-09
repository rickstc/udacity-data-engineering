# Generated by Django 5.0 on 2024-01-04 03:31

import django.contrib.gis.db.models.fields
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='WeatherStation',
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
        migrations.CreateModel(
            name='Athlete',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=128)),
                ('deduplication_number', models.PositiveSmallIntegerField(default=0)),
                ('gender', models.CharField(choices=[('M', 'Male'), ('F', 'Female'), ('X', 'Mx')], default='X', max_length=1)),
            ],
            options={
                'unique_together': {('name', 'deduplication_number')},
            },
        ),
        migrations.CreateModel(
            name='ContestLocation',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('country', models.CharField(max_length=128)),
                ('state', models.CharField(blank=True, default='', max_length=128)),
                ('town', models.CharField(blank=True, default='', max_length=128)),
                ('location', django.contrib.gis.db.models.fields.PointField(blank=True, default=None, null=True, srid=4326)),
                ('population', models.PositiveIntegerField(default=0)),
            ],
            options={
                'unique_together': {('country', 'state', 'town')},
            },
        ),
        migrations.CreateModel(
            name='Contest',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=256)),
                ('federation', models.CharField(max_length=64)),
                ('parent_federation', models.CharField(blank=True, default='', max_length=64)),
                ('date', models.DateField()),
                ('location', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='contests', to='fact.contestlocation')),
            ],
        ),
        migrations.CreateModel(
            name='ContestResult',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('event', models.CharField(choices=[('FP', 'Squat-Bench-Deadlift'), ('BD', 'Bench-Deadlift'), ('SD', 'Squat-Deadlift'), ('SB', 'Squat-Bench'), ('SQ', 'Squat'), ('BP', 'Bench'), ('DL', 'Deadlift')], max_length=2)),
                ('equipment', models.CharField(choices=[('R', 'Raw (Bare knees or knee sleeves)'), ('W', 'Wraps (Knee wraps are allowed)'), ('S', 'Single-ply (Equipped, single-ply suits)'), ('M', 'Multi-ply (Equipped, multi-ply suits)'), ('U', 'Unlimited (Equipped, multi-ply suits or rubberized gear)'), ('T', 'Straps (Allowed straps on the deadlift)')], max_length=1)),
                ('drug_tested', models.BooleanField(default=False)),
                ('division', models.CharField(blank=True, default='', max_length=64)),
                ('age', models.DecimalField(decimal_places=1, default=0, max_digits=4)),
                ('age_class', models.CharField(blank=True, default='', max_length=6)),
                ('birth_year_class', models.CharField(blank=True, default='', max_length=6)),
                ('bodyweight', models.DecimalField(decimal_places=2, default=0, max_digits=5)),
                ('weight_class', models.DecimalField(decimal_places=2, default=0, max_digits=5)),
                ('deadlift', models.DecimalField(decimal_places=2, default=0, max_digits=5)),
                ('squat', models.DecimalField(decimal_places=2, default=0, max_digits=5)),
                ('bench_press', models.DecimalField(decimal_places=2, default=0, max_digits=5)),
                ('place', models.PositiveSmallIntegerField(default=0)),
                ('meet_total', models.DecimalField(decimal_places=2, default=0, max_digits=6)),
                ('dots', models.DecimalField(decimal_places=2, default=0, max_digits=5)),
                ('athlete', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='results', to='fact.athlete')),
                ('contest', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='results', to='fact.contest')),
            ],
        ),
    ]
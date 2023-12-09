# Generated by Django 4.2.7 on 2023-12-09 15:27

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Station',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('station_id', models.CharField(max_length=11, unique=True)),
                ('elevation', models.DecimalField(decimal_places=1, max_digits=6)),
                ('state', models.CharField(blank=True, max_length=2)),
                ('name', models.CharField(max_length=30)),
                ('gsn_flag', models.BooleanField(default=False)),
                ('wmo_id', models.PositiveIntegerField(blank=True, unique=True)),
            ],
        ),
    ]

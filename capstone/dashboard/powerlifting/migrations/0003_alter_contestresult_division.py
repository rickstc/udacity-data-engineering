# Generated by Django 4.2.7 on 2023-12-04 01:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('powerlifting', '0002_alter_contest_name'),
    ]

    operations = [
        migrations.AlterField(
            model_name='contestresult',
            name='division',
            field=models.CharField(blank=True, default='', max_length=64),
        ),
    ]

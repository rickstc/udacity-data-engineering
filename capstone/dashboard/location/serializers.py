from rest_framework import serializers
from location import models


class StationSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Station
        fields = "__all__"


class CityLocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.CityLocation
        fields = "__all__"

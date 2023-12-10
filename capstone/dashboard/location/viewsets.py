from rest_framework import viewsets, mixins
from location import models
from location import serializers


class StationViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Station.objects.all()
    serializer_class = serializers.StationSerializer


class CityLocationViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.CityLocation.objects.all()
    serializer_class = serializers.CityLocationSerializer

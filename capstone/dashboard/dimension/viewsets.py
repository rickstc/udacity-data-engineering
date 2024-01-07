from rest_framework import viewsets, mixins
from dimension import models, serializers


class ContestViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Contest.objects.all()
    serializer_class = serializers.ContestSerializer


class LocationViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Location.objects.all()
    serializer_class = serializers.LocationSerializer

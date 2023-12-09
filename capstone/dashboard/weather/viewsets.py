from rest_framework import viewsets, mixins
from weather import models, serializers


class StationViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Station.objects.all()
    serializer_class = serializers.StationSerializer

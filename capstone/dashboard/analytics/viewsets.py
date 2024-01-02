from rest_framework import viewsets, mixins
from analytics import models
from analytics import serializers


class ContestViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Contest.objects.all()
    serializer_class = serializers.ContestSerializer

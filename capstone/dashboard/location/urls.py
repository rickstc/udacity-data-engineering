from rest_framework import routers
from location import viewsets

app_name = "location"
router = routers.DefaultRouter()
router.register(r"stations", viewsets.StationViewSet, basename="station")
router.register(r"cities", viewsets.CityLocationViewSet, basename="city")

urlpatterns = router.urls

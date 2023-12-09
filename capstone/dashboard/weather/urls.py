from rest_framework import routers
from weather import viewsets

app_name = "weather"
router = routers.DefaultRouter()
router.register(r"stations", viewsets.StationViewSet, basename="station")

urlpatterns = router.urls

from rest_framework import routers
from dimension import viewsets

app_name = "dimension"

router = routers.DefaultRouter()
router.register(r"contests", viewsets.ContestViewSet, basename="contest")
urlpatterns = router.urls

from django.urls import path

from . import views

app_name = "summary"
urlpatterns = [
    path("", views.index, name="index"),
    # path("<int:materialID_num>/", views.detail, name="detail"),
    path("<str:materialID_str>/", views.detail_dash, name="detail_dash"),
]
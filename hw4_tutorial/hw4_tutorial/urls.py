from django.urls import path
from . import views

urlpatterns = [
    path('hello', views.hello),
    path('dashboard', views.dashboard),
    path('connection', views.connection)
]
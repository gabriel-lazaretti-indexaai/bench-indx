FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY BenchmarkService.csproj ./
RUN dotnet restore BenchmarkService.csproj
COPY . ./
RUN dotnet publish BenchmarkService.csproj -c Release -o /out --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /out ./
ENV ASPNETCORE_URLS=http://+:3000
EXPOSE 3000
ENTRYPOINT ["dotnet", "BenchmarkService.dll"]

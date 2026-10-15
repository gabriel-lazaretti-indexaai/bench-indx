import { Module } from "@nestjs/common";
import { BenchmarkController, HealthController } from "./benchmark.controller";
import { BenchmarkService } from "./benchmark.service";

@Module({
  controllers: [BenchmarkController, HealthController],
  providers: [BenchmarkService],
})
export class AppModule {}

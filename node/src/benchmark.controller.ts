import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
} from "@nestjs/common";
import { CreateBenchmarkJobDto } from "./benchmark.dto";
import { BenchmarkService } from "./benchmark.service";

@Controller()
export class HealthController {
  @Get("health")
  health(): {
    status: string;
    runtime: string;
    runtimeVersion: string;
    framework: string;
    frameworkVersion: string;
  } {
    const nestPackage = require("@nestjs/core/package.json") as { version: string };
    return {
      status: "ok",
      runtime: "node",
      runtimeVersion: process.version,
      framework: "NestJS",
      frameworkVersion: nestPackage.version,
    };
  }
}

@Controller("benchmark")
export class BenchmarkController {
  constructor(private readonly benchmarkService: BenchmarkService) {}

  @Post("/jobs")
  @HttpCode(HttpStatus.ACCEPTED)
  createJob(@Body() input: CreateBenchmarkJobDto) {
    return this.benchmarkService.createJob(input);
  }

  @Get("/jobs/:jobId")
  getJob(@Param("jobId") jobId: string) {
    return this.benchmarkService.getJob(jobId);
  }
}

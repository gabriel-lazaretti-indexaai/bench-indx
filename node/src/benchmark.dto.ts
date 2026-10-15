import { Type } from "class-transformer";
import {
  IsIn,
  IsInt,
  IsString,
  Max,
  Min,
  ValidateIf,
} from "class-validator";

export const WORKFLOWS = ["approval", "enrichment", "synchronization"] as const;
export type Workflow = (typeof WORKFLOWS)[number];

export class CreateBenchmarkJobDto {
  @IsString()
  @IsIn(WORKFLOWS)
  workflow!: Workflow;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(5000)
  itemCount!: number;

  @ValidateIf((_object, value) => value !== undefined)
  @Type(() => Number)
  @IsInt()
  @Min(0)
  @Max(200)
  externalLatencyMs = 0;

  @ValidateIf((_object, value) => value !== undefined)
  @Type(() => Number)
  @IsInt()
  @Min(0)
  @Max(2147483647)
  failureEvery = 0;
}

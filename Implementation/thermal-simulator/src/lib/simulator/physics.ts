import calibratedParams from './calibrated_physics.json';

export const PHYSICS_PARAMS = {
  DT: 0.11,
  ...calibratedParams
};

export interface ThermalState {
  T_die_0: number;
  T_die_1: number;
  T_sink_0: number;
  T_sink_1: number;
}

export interface DynamicCoolingParams {
  h_base_0: number;
  h_active_0: number;
  h_base_1: number;
  h_active_1: number;
}

const INV_C_DIE_0 = 1.0 / PHYSICS_PARAMS.C_die_0;
const INV_C_DIE_1 = 1.0 / PHYSICS_PARAMS.C_die_1;
const INV_C_SINK_0 = 1.0 / PHYSICS_PARAMS.C_sink_0;
const INV_C_SINK_1 = 1.0 / PHYSICS_PARAMS.C_sink_1;
const INV_R_PASTE_0 = 1.0 / PHYSICS_PARAMS.R_paste_0;
const INV_R_PASTE_1 = 1.0 / PHYSICS_PARAMS.R_paste_1;

function sigmoid(x: number): number {
  return 1.0 / (1.0 + Math.exp(-x));
}

export function getPrecalculatedParams(coolingEfficiencyPct: number): DynamicCoolingParams {
  const mult = coolingEfficiencyPct / 100.0;
  return {
    h_base_0: PHYSICS_PARAMS.h_base_0 * mult,
    h_active_0: PHYSICS_PARAMS.h_active_0 * mult,
    h_base_1: PHYSICS_PARAMS.h_base_1 * mult,
    h_active_1: PHYSICS_PARAMS.h_active_1 * mult,
  };
}

export function stepPhysics(
  state: ThermalState,
  P0: number,
  P1: number,
  T_amb: number,
  coolingParams: DynamicCoolingParams 
): ThermalState {
  const p = PHYSICS_PARAMS;

  const h0_curr = coolingParams.h_base_0 + coolingParams.h_active_0 * sigmoid(p.beta_0 * (state.T_die_0 - p.T_thresh_0));
  const h1_curr = coolingParams.h_base_1 + coolingParams.h_active_1 * sigmoid(p.beta_1 * (state.T_die_1 - p.T_thresh_1));

  const dT0_die = (P0 - (state.T_die_0 - state.T_sink_0) * INV_R_PASTE_0) * INV_C_DIE_0;
  const dT0_sink = (
    (state.T_die_0 - state.T_sink_0) * INV_R_PASTE_0 + 
    p.k01 * P1 - 
    h0_curr * (state.T_sink_0 - T_amb) + 
    p.q0
  ) * INV_C_SINK_0;

  const dT1_die = (P1 - (state.T_die_1 - state.T_sink_1) * INV_R_PASTE_1) * INV_C_DIE_1;
  const dT1_sink = (
    (state.T_die_1 - state.T_sink_1) * INV_R_PASTE_1 + 
    p.k10 * P0 - 
    h1_curr * (state.T_sink_1 - T_amb) + 
    p.q1
  ) * INV_C_SINK_1;

  return {
    T_die_0: state.T_die_0 + p.DT * dT0_die,
    T_die_1: state.T_die_1 + p.DT * dT1_die,
    T_sink_0: state.T_sink_0 + p.DT * dT0_sink,
    T_sink_1: state.T_sink_1 + p.DT * dT1_sink,
  };
}
// 1. Import the dynamically calibrated parameters from PyTorch
import calibratedParams from './calibrated_physics.json';

// 2. Combine the JSON weights with our fixed simulation timestep (DT)
export const PHYSICS_PARAMS = {
  DT: 0.11, // 110ms timestep from the MIT dataset
  ...calibratedParams
};

// 3. Inverse C calculations (calculated once at startup for maximum loop speed)
const INV_C_DIE_0 = 1.0 / PHYSICS_PARAMS.C_die_0;
const INV_C_DIE_1 = 1.0 / PHYSICS_PARAMS.C_die_1;
const INV_C_SINK_0 = 1.0 / PHYSICS_PARAMS.C_sink_0;
const INV_C_SINK_1 = 1.0 / PHYSICS_PARAMS.C_sink_1;

// 4. TypeScript Interface for a Server Node's Thermal State
export interface ThermalState {
  T_die_0: number;
  T_die_1: number;
  T_sink_0: number;
  T_sink_1: number;
}

// 5. Math Helper: Standard Sigmoid Function (identical to torch.sigmoid)
function sigmoid(x: number): number {
  return 1.0 / (1.0 + Math.exp(-x));
}

/**
 * 6. The Core 2-Mass Forward Euler Integration Step
 * Takes the current temperatures and power draw, steps forward by DT (0.11s), 
 * and returns the new temperatures.
 */
export function stepPhysics(
  state: ThermalState,
  P0: number,
  P1: number,
  T_amb: number
): ThermalState {
  const p = PHYSICS_PARAMS;

  // Calculate dynamic active cooling (Fan curves)
  const h0_curr = p.h_base_0 + p.h_active_0 * sigmoid(p.beta_0 * (state.T_die_0 - p.T_thresh_0));
  const h1_curr = p.h_base_1 + p.h_active_1 * sigmoid(p.beta_1 * (state.T_die_1 - p.T_thresh_1));

  // Calculate derivatives (Slopes) for GPU 0
  const dT0_die = (P0 - (state.T_die_0 - state.T_sink_0) / p.R_paste_0) * INV_C_DIE_0;
  const dT0_sink = (
    (state.T_die_0 - state.T_sink_0) / p.R_paste_0 + 
    p.k01 * P1 - 
    h0_curr * (state.T_sink_0 - T_amb) + 
    p.q0
  ) * INV_C_SINK_0;

  // Calculate derivatives (Slopes) for GPU 1
  const dT1_die = (P1 - (state.T_die_1 - state.T_sink_1) / p.R_paste_1) * INV_C_DIE_1;
  const dT1_sink = (
    (state.T_die_1 - state.T_sink_1) / p.R_paste_1 + 
    p.k10 * P0 - 
    h1_curr * (state.T_sink_1 - T_amb) + 
    p.q1
  ) * INV_C_SINK_1;

  // Apply Forward Euler Step (T_new = T_old + DT * derivative)
  return {
    T_die_0: state.T_die_0 + p.DT * dT0_die,
    T_die_1: state.T_die_1 + p.DT * dT1_die,
    T_sink_0: state.T_sink_0 + p.DT * dT0_sink,
    T_sink_1: state.T_sink_1 + p.DT * dT1_sink,
  };
}
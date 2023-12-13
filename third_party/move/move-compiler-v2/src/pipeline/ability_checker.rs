//! Checks for ability violations.
//! prerequisite:
//! - liveness analysis and lifetime analysis have been performed
//! - Copies and moves have been made explicit in assignment instructions

use itertools::Itertools;
use move_binary_format::file_format::{Ability, AbilitySet};
use move_model::{
    ast::TempIndex,
    model::{FunId, FunctionEnv, Loc, ModuleId, QualifiedId, StructId, TypeParameter},
    ty,
    ty::Type,
};
use move_stackless_bytecode::{
    function_target::{FunctionData, FunctionTarget},
    function_target_pipeline::{FunctionTargetProcessor, FunctionTargetsHolder},
    stackless_bytecode::{AssignKind, Bytecode, Operation},
};

/// Returns the abilities of the given type.
fn type_abilities(func_target: &FunctionTarget, ty: &Type) -> AbilitySet {
    let ty_params = func_target.get_type_parameters();
    let global_env = func_target.global_env();
    global_env.type_abilities(ty, &ty_params)
}

/// Determines if the given type has constraint copy.
fn has_copy(func_target: &FunctionTarget, ty: &Type) -> bool {
    type_abilities(func_target, ty).has_ability(Ability::Copy)
}

/// Checks if the given type has constraint copy, and add diagnostics if not.
fn check_copy(func_target: &FunctionTarget, ty: &Type, loc: &Loc, err_msg: &str) {
    if !has_copy(func_target, ty) {
        func_target.global_env().error(loc, err_msg)
    }
}

/// Checks if the given temporary variable has constraint copy, and add diagnostics if not.
fn check_copy_for_temp_with_msg(
    func_target: &FunctionTarget,
    t: TempIndex,
    loc: &Loc,
    err_msg: &str,
) {
    let ty = func_target.get_local_type(t);
    check_copy(func_target, ty, loc, err_msg)
}

/// `t` is the local containing the reference read
fn check_read_ref(target: &FunctionTarget, t: TempIndex, loc: &Loc) {
    if let Type::Reference(_, ty) = target.get_local_type(t) {
        check_copy(target, ty, loc, "cannot copy")
    } else {
        panic!("ICE from ability checker: read_ref has non-reference argument")
    }
}

/// Determines if the given type has the drop constraint.
fn has_drop(func_target: &FunctionTarget, ty: &Type) -> bool {
    type_abilities(func_target, ty).has_ability(Ability::Drop)
}

/// Checks if the given type has constraint drop, and add diagnostics if not.
fn check_drop(func_target: &FunctionTarget, ty: &Type, loc: &Loc, err_msg: &str) {
    if !has_drop(func_target, ty) {
        func_target.global_env().error(loc, err_msg)
    }
}

/// Checks if the given temporary variable has constraint drop, and add diagnostics if not.
fn check_drop_for_temp_with_msg(
    func_target: &FunctionTarget,
    t: TempIndex,
    loc: &Loc,
    err_msg: &str,
) {
    let ty = func_target.get_local_type(t);
    check_drop(func_target, ty, loc, err_msg)
}

/// `t` is the local containing the reference being written to
fn check_write_ref(target: &FunctionTarget, t: TempIndex, loc: &Loc) {
    if let Type::Reference(_, ty) = target.get_local_type(t) {
        check_drop(target, ty, loc, "write_ref: cannot drop")
    } else {
        panic!("ICE typing error")
    }
}

fn check_key_for_struct(
    target: &FunctionTarget,
    mod_id: ModuleId,
    struct_id: StructId,
    insts: &[Type],
    loc: &Loc,
    err_msg: &str,
) {
    if !check_struct_inst(target, mod_id, struct_id, insts, &loc).has_ability(Ability::Key) {
        target.global_env().error(loc, err_msg)
    }
}

fn ty_param_abilities(ty_params: &[TypeParameter]) -> impl Fn(u16) -> AbilitySet + Copy + '_ {
    |i| {
        if let Some(tp) = ty_params.get(i as usize) {
            tp.1.abilities
        } else {
            panic!("ICE unbound type parameter")
        }
    }
}

fn get_abilities<'a>(
    target: &'a FunctionTarget,
) -> impl Fn(ModuleId, StructId) -> (Vec<AbilitySet>, AbilitySet) + Copy + 'a {
    |mid, sid| {
        let qid = QualifiedId {
            module_id: mid,
            id: sid,
        };
        let struct_env = target.global_env().get_struct(qid);
        let struct_abilities = struct_env.get_abilities();
        let params_ability_constraints = struct_env
            .get_type_parameters()
            .iter()
            .map(|tp| tp.1.abilities)
            .collect_vec();
        (params_ability_constraints, struct_abilities)
    }
}

/// checks that the given type is instantiated with types satisfying their ability constraints
/// on the type parameter
fn check_struct_inst(
    target: &FunctionTarget,
    mid: ModuleId,
    sid: StructId,
    ty_args: &[Type],
    loc: &Loc,
) -> AbilitySet {
    let ty_params = target.get_type_parameters();
    ty::check_struct_inst(
        mid,
        sid,
        ty_args,
        ty_param_abilities(&ty_params),
        get_abilities(target),
        Some((loc, |loc: &Loc, msg: &str| target.global_env().error(loc, msg))),
    )
}

fn check_fun_inst(target: &FunctionTarget, mid: ModuleId, fid: FunId, inst: &[Type], loc: &Loc) {
    let qid = QualifiedId {
        module_id: mid,
        id: fid,
    };
    let fun_env = target.global_env().get_function(qid);
    for (param, ty) in fun_env.get_type_parameters().iter().zip(inst.iter()) {
        let required_abilities = param.1.abilities;
        let given_abilities = check_instantiation(target, ty, loc);
        // TODO: which field, why
        if !required_abilities.is_subset(given_abilities) {
            target.global_env().error(loc, "invalid instantiation")
        }
    }
}

/// `ty::infer_and_check_abilities` in the context of a `FunctionTarget`
/// where the abilities of type parameters comes from the function generics
pub fn check_instantiation(target: &FunctionTarget, ty: &Type, loc: &Loc) -> AbilitySet {
    let ty_params = target.get_type_parameters();
    ty::infer_and_check_abilities(
        ty,
        ty_param_abilities(&ty_params),
        get_abilities(target),
        loc,
        |loc, msg| target.global_env().error(loc, msg),
    )
}

pub struct AbilityChecker();

impl FunctionTargetProcessor for AbilityChecker {
    fn process(
        &self,
        _targets: &mut FunctionTargetsHolder,
        fun_env: &FunctionEnv,
        data: FunctionData,
        _scc_opt: Option<&[FunctionEnv]>,
    ) -> FunctionData {
        if fun_env.is_native() {
            return data;
        }
        let target = FunctionTarget::new(fun_env, &data);
        check_fun_signature(&target);
        for bytecode in target.get_bytecode() {
            check_bytecode(&target, bytecode)
        }
        data
    }

    fn name(&self) -> String {
        "AbilityChecker".to_owned()
    }
}

fn check_fun_signature(target: &FunctionTarget) {
    for param in target.get_parameters() {
        let param_ty = target.get_local_type(param);
        // TODO: provide more accurate location
        check_instantiation(target, param_ty, &target.get_loc());
    }
    // return type is checked in function body
}

fn check_bytecode(target: &FunctionTarget, bytecode: &Bytecode) {
    let loc = target.get_bytecode_loc(bytecode.get_attr_id());
    match bytecode {
        Bytecode::Assign(_, dst, src, kind) => {
            // drop of dst during the assignment has been made explicit
            // so we don't check it here, plus this could be an initialization
            match kind {
                AssignKind::Copy | AssignKind::Store => {
                    check_copy_for_temp_with_msg(target, *src, &loc, "cannot copy");
                    // dst is not dropped in advande in this case, since it's read by src
                    if *dst == *src {
                        check_drop_for_temp_with_msg(target, *dst, &loc, "invalid implicit drop")
                    }
                }
                AssignKind::Move => (),
                AssignKind::Inferred => {
                    panic!("ICE ability checker given inferred assignment")
                }
            }
        },
        Bytecode::Call(attr_id, _, op, srcs, _) => {
            use Operation::*;
            let loc = target.get_bytecode_loc(*attr_id);
            match op {
                Function(mod_id, fun_id, insts) => {
                    check_fun_inst(target, *mod_id, *fun_id, insts, &loc);
                },
                Pack(mod_id, struct_id, insts) => {
                    check_struct_inst(target, *mod_id, *struct_id, insts, &loc);
                },
                Unpack(mod_id, struct_id, insts) => {
                    check_struct_inst(target, *mod_id, *struct_id, insts, &loc);
                },
                MoveTo(mod_id, struct_id, insts) => {
                    check_key_for_struct(target, *mod_id, *struct_id, insts, &loc, "no key ability")
                },
                MoveFrom(mod_id, struct_id, insts) => {
                    check_key_for_struct(target, *mod_id, *struct_id, insts, &loc, "no key ability")
                },
                Exists(mod_id, struct_id, insts) => {
                    check_key_for_struct(target, *mod_id, *struct_id, insts, &loc, "no key ability")
                },
                BorrowGlobal(mod_id, struct_id, insts) => {
                    check_key_for_struct(target, *mod_id, *struct_id, insts, &loc, "no key ability")
                },
                BorrowField(mod_id, struct_id, insts, _) => {
                    check_struct_inst(target, *mod_id, *struct_id, insts, &loc);
                },
                Destroy => check_drop_for_temp_with_msg(target, srcs[0], &loc, "cannot drop"),
                ReadRef => check_read_ref(target, srcs[0], &loc),
                WriteRef => check_write_ref(target, srcs[0], &loc),
                _ => (),
            }
        },
        _ => (),
    }
}
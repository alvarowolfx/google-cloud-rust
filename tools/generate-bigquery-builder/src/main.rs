// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use quote::{quote, ToTokens};
use syn::{File as SynFile, Item, ImplItem, Visibility};

#[derive(Clone, Debug)]
struct MethodInfo {
    name: String,
    sig: syn::Signature,
    attrs: Vec<syn::Attribute>,
}

const SKIPPED_SETTERS: &[&str] = &["set_query", "set_kind"];
const OUTPUT_ONLY_SETTERS: &[&str] = &["set_job_type", "set_or_clear_job_type"];

fn main() -> anyhow::Result<()> {
    let model_path = Path::new("src/generated/cloud/bigquery/v2/src/model.rs");
    println!("Reading model from: {:?}", model_path);

    let mut content = String::new();
    File::open(model_path)?.read_to_string(&mut content)?;

    println!("Parsing model AST...");
    let ast: SynFile = syn::parse_file(&content)?;

    let mut query_request_methods = HashMap::new();
    let mut job_config_query_methods = HashMap::new();
    let mut job_config_methods = HashMap::new();

    for item in ast.items {
        if let Item::Impl(item_impl) = item {
            let self_ty = &item_impl.self_ty;
            let type_name = quote!(#self_ty).to_string();

            if type_name == "QueryRequest" {
                for impl_item in item_impl.items {
                    if let ImplItem::Fn(method) = impl_item {
                        if matches!(method.vis, Visibility::Public(_)) {
                            let name = method.sig.ident.to_string();
                            if name.starts_with("set_") || name.starts_with("set_or_clear_") {
                                query_request_methods.insert(name.clone(), MethodInfo {
                                    name,
                                    sig: method.sig.clone(),
                                    attrs: method.attrs.clone(),
                                });
                            }
                        }
                    }
                }
            } else if type_name == "JobConfigurationQuery" {
                for impl_item in item_impl.items {
                    if let ImplItem::Fn(method) = impl_item {
                        if matches!(method.vis, Visibility::Public(_)) {
                            let name = method.sig.ident.to_string();
                            if name.starts_with("set_") || name.starts_with("set_or_clear_") {
                                job_config_query_methods.insert(name.clone(), MethodInfo {
                                    name,
                                    sig: method.sig.clone(),
                                    attrs: method.attrs.clone(),
                                });
                            }
                        }
                    }
                }
            } else if type_name == "JobConfiguration" {
                for impl_item in item_impl.items {
                    if let ImplItem::Fn(method) = impl_item {
                        if matches!(method.vis, Visibility::Public(_)) {
                            let name = method.sig.ident.to_string();
                            if name.starts_with("set_") || name.starts_with("set_or_clear_") {
                                // Skip non-query configurations (copy, load, extract)
                                if name.contains("_copy") || name.contains("_load") || name.contains("_extract") {
                                    continue;
                                }
                                job_config_methods.insert(name.clone(), MethodInfo {
                                    name,
                                    sig: method.sig.clone(),
                                    attrs: method.attrs.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    println!("Found {} setters on QueryRequest", query_request_methods.len());
    println!("Found {} setters on JobConfigurationQuery", job_config_query_methods.len());
    println!("Found {} setters on JobConfiguration", job_config_methods.len());

    let out_dir = Path::new("src/bigquery/src/query");
    std::fs::create_dir_all(out_dir)?;
    let out_path = out_dir.join("generated_builder.rs");
    let mut out_file = File::create(&out_path)?;

    // Write header
    writeln!(
        out_file,
        "// Copyright 2026 Google LLC\n\
         //\n\
         // Licensed under the Apache License, Version 2.0 (the \"License\");\n\
         // you may not use this file except in compliance with the License.\n\
         // You may obtain a copy of the License at\n\
         //\n\
         //     https://www.apache.org/licenses/LICENSE-2.0\n\
         //\n\
         // Unless required by applicable law or agreed to in writing, software\n\
         // distributed under the License is distributed on an \"AS IS\" BASIS,\n\
         // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
         // See the License for the specific language governing permissions and\n\
         // limitations under the License.\n\n\
         // AUTO-GENERATED CODE. DO NOT EDIT MANUALLY. RUN THE GENERATOR SCRIPT TO UPDATE.\n\n\
         use super::RunQuery;\n\n\
         #[allow(clippy::clone_on_copy)]\n\
         impl RunQuery {{",
    )?;

    let mut sorted_names: Vec<String> = query_request_methods.keys()
        .chain(job_config_query_methods.keys())
        .chain(job_config_methods.keys())
        .cloned()
        .collect();
    sorted_names.sort();
    sorted_names.dedup();

    for name in sorted_names {
        // Skip special/internal/output-only methods
        if SKIPPED_SETTERS.contains(&name.as_str()) || OUTPUT_ONLY_SETTERS.contains(&name.as_str()) {
            continue;
        }

        let on_qr = query_request_methods.get(&name);
        let on_jcq = job_config_query_methods.get(&name);
        let on_jc = job_config_methods.get(&name);

        let is_or_clear = name.starts_with("set_or_clear_");

        // Determine how to generate the setter
        if let Some(qr) = on_qr {
            let docs = get_docs(&qr.attrs);
            let sig = &qr.sig;
            let sig_str = sig.to_token_stream().to_string()
                .replace("crate :: model", "google_cloud_bigquery_v2 :: model")
                .replace("crate::model", "google_cloud_bigquery_v2::model");

            if let Some(jc) = on_jc {
                // Common to QueryRequest and JobConfiguration (e.g., set_dry_run)
                let body = if name == "set_labels" {
                    format!(
                        "        let val: std::collections::HashMap<std::string::String, std::string::String> = v.into_iter().map(|(k, v)| (k.into(), v.into())).collect();\n\
                         产self.query_request = self.query_request.set_labels(val.clone());\n\
                         产self.job_config = self.job_config.set_labels(val);\n\
                         产self"
                    ).replace("产", " ")
                } else if let Some((target_ty, is_iter)) = extract_target_type(sig) {
                    let target_ty_str = target_ty.to_token_stream().to_string()
                        .replace("crate :: model", "google_cloud_bigquery_v2 :: model")
                        .replace("crate::model", "google_cloud_bigquery_v2::model");
                    if is_iter {
                        format!(
                            "        let val: Vec<{}> = v.into_iter().map(|i| i.into()).collect();\n\
                             产self.query_request = self.query_request.{}(val.clone());\n\
                             产self.job_config = self.job_config.{}(val);\n\
                             产self",
                            target_ty_str, name, name
                        ).replace("产", " ")
                    } else if is_or_clear {
                        format!(
                            "        let val: Option<{}> = v.map(|x| x.into());\n\
                             产self.query_request = self.query_request.{}(val.clone());\n\
                             产self.job_config = self.job_config.{}(val);\n\
                             产self",
                            target_ty_str, name, name
                        ).replace("产", " ")
                    } else {
                        format!(
                            "        let val: {} = v.into();\n\
                             产self.query_request = self.query_request.{}(val.clone());\n\
                             产self.job_config = self.job_config.{}(val);\n\
                             产self",
                            target_ty_str, name, name
                        ).replace("产", " ")
                    }
                } else {
                    format!(
                        "        self.query_request = self.query_request.{}(v.clone());\n\
                         产self.job_config = self.job_config.{}(v);\n\
                         产self",
                        name, name
                    ).replace("产", " ")
                };
                writeln!(out_file, "\n{}    pub {} {{\n{}\n    }}", docs, sig_str, body)?;

            } else if let Some(jcq) = on_jcq {
                // Common to QueryRequest and JobConfigurationQuery (e.g., set_default_dataset)
                let body = if let Some((target_ty, is_iter)) = extract_target_type(sig) {
                    let target_ty_str = target_ty.to_token_stream().to_string()
                        .replace("crate :: model", "google_cloud_bigquery_v2 :: model")
                        .replace("crate::model", "google_cloud_bigquery_v2::model");
                    if is_iter {
                        format!(
                            "        let val: Vec<{}> = v.into_iter().map(|i| i.into()).collect();\n\
                             产self.query_request = self.query_request.{}(val.clone());\n\
                             产let mut q = self.job_config.query.take().unwrap_or_default();\n\
                             产q = q.{}(val);\n\
                             产self.job_config.query = Some(q);\n\
                             产self",
                            target_ty_str, name, name
                        ).replace("产", " ")
                    } else if is_or_clear {
                        format!(
                            "        let val: Option<{}> = v.map(|x| x.into());\n\
                             产self.query_request = self.query_request.{}(val.clone());\n\
                             产let mut q = self.job_config.query.take().unwrap_or_default();\n\
                             产q = q.{}(val);\n\
                             产self.job_config.query = Some(q);\n\
                             产self",
                            target_ty_str, name, name
                        ).replace("产", " ")
                    } else {
                        format!(
                            "        let val: {} = v.into();\n\
                             产self.query_request = self.query_request.{}(val.clone());\n\
                             产let mut q = self.job_config.query.take().unwrap_or_default();\n\
                             产q = q.{}(val);\n\
                             产self.job_config.query = Some(q);\n\
                             产self",
                            target_ty_str, name, name
                        ).replace("产", " ")
                    }
                } else {
                    format!(
                        "        self.query_request = self.query_request.{}(v.clone());\n\
                         产let mut q = self.job_config.query.take().unwrap_or_default();\n\
                         产q = q.{}(v);\n\
                         产self.job_config.query = Some(q);\n\
                         产self",
                        name, name
                    ).replace("产", " ")
                };
                writeln!(out_file, "\n{}    pub {} {{\n{}\n    }}", docs, sig_str, body)?;

            } else {
                // Unique to QueryRequest (e.g., set_max_results)
                let body = format!(
                    "        self.query_request = self.query_request.{}(v);\n\
                     产self",
                    name
                ).replace("产", " ");
                writeln!(out_file, "\n{}    pub {} {{\n{}\n    }}", docs, sig_str, body)?;
            }

        } else if let Some(jcq) = on_jcq {
            // Unique to JobConfigurationQuery (e.g., set_destination_table)
            let docs = get_docs(&jcq.attrs);
            let sig = &jcq.sig;
            let sig_str = sig.to_token_stream().to_string()
                .replace("crate :: model", "google_cloud_bigquery_v2 :: model")
                .replace("crate::model", "google_cloud_bigquery_v2::model");

            let body = format!(
                "        let mut q = self.job_config.query.take().unwrap_or_default();\n\
                 产q = q.{}(v);\n\
                 产self.job_config.query = Some(q);\n\
                 产self.force_job_path = true;\n\
                 产self",
                name
            ).replace("产", " ");
            writeln!(out_file, "\n{}    pub {} {{\n{}\n    }}", docs, sig_str, body)?;

        } else if let Some(jc) = on_jc {
            // Unique to JobConfiguration (if any, e.g., other top-level fields)
            let docs = get_docs(&jc.attrs);
            let sig = &jc.sig;
            let sig_str = sig.to_token_stream().to_string()
                .replace("crate :: model", "google_cloud_bigquery_v2 :: model")
                .replace("crate::model", "google_cloud_bigquery_v2::model");

            let body = format!(
                "        self.job_config = self.job_config.{}(v);\n\
                 产self.force_job_path = true;\n\
                 产self",
                name
            ).replace("产", " ");
            writeln!(out_file, "\n{}    pub {} {{\n{}\n    }}", docs, sig_str, body)?;
        }
    }

    writeln!(out_file, "}}")?;
    println!("Successfully generated builder code at {:?}", out_path);

    Ok(())
}

fn get_docs(attrs: &[syn::Attribute]) -> String {
    let mut docs = String::new();
    for attr in attrs {
        if attr.path().is_ident("doc") {
            if let syn::Meta::NameValue(nv) = &attr.meta {
                if let syn::Expr::Lit(expr_lit) = &nv.value {
                    if let syn::Lit::Str(lit_str) = &expr_lit.lit {
                        docs.push_str(&format!("    /// {}\n", lit_str.value().trim_start()));
                    }
                }
            }
        }
    }
    docs
}

fn extract_target_type(sig: &syn::Signature) -> Option<(syn::Type, bool)> {
    if let Some(where_clause) = &sig.generics.where_clause {
        for pred in &where_clause.predicates {
            if let syn::WherePredicate::Type(pred_type) = pred {
                if let syn::Type::Path(type_path) = &pred_type.bounded_ty {
                    if type_path.path.is_ident("T") {
                        for bound in &pred_type.bounds {
                            if let syn::TypeParamBound::Trait(trait_bound) = bound {
                                let segments = &trait_bound.path.segments;
                                if let Some(last) = segments.last() {
                                    if last.ident == "Into" {
                                        if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                                            if let Some(syn::GenericArgument::Type(ty)) = args.args.first() {
                                                return Some((ty.clone(), false));
                                            }
                                        }
                                    } else if last.ident == "IntoIterator" {
                                        let mut item_type = None;
                                        if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                                            for arg in &args.args {
                                                if let syn::GenericArgument::AssocType(assoc) = arg {
                                                    if assoc.ident == "Item" {
                                                        item_type = Some(assoc.ty.clone());
                                                    }
                                                }
                                            }
                                        }
                                        if let Some(item_ty) = item_type {
                                            if let syn::Type::Path(item_path) = &item_ty {
                                                if let Some(v_ident) = item_path.path.get_ident() {
                                                    let v_name = v_ident.to_string();
                                                    for pred2 in &where_clause.predicates {
                                                        if let syn::WherePredicate::Type(pt2) = pred2 {
                                                            if let syn::Type::Path(pt2_path) = &pt2.bounded_ty {
                                                                if pt2_path.path.is_ident(&v_name) {
                                                                    for b2 in &pt2.bounds {
                                                                        if let syn::TypeParamBound::Trait(tb2) = b2 {
                                                                            let segs2 = &tb2.path.segments;
                                                                            if let Some(l2) = segs2.last() {
                                                                                if l2.ident == "Into" {
                                                                                    if let syn::PathArguments::AngleBracketed(a2) = &l2.arguments {
                                                                                        if let Some(syn::GenericArgument::Type(t2)) = a2.args.first() {
                                                                                            return Some((t2.clone(), true));
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    for param in &sig.generics.params {
        if let syn::GenericParam::Type(type_param) = param {
            if type_param.ident == "T" {
                for bound in &type_param.bounds {
                    if let syn::TypeParamBound::Trait(trait_bound) = bound {
                        let segments = &trait_bound.path.segments;
                        if let Some(last) = segments.last() {
                            if last.ident == "Into" {
                                if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                                    if let Some(syn::GenericArgument::Type(ty)) = args.args.first() {
                                        return Some((ty.clone(), false));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

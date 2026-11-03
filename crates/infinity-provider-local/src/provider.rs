use crate::{BoxError, config::Config};
use async_trait::async_trait;
use infinity_provider_protocol::{
    CompletionError, CompletionRequest, ErrorClass, ModelEntry, ModelProvider, ModelStream,
};

pub struct LocalProvider {
    config: Config,
}
impl LocalProvider {
    pub fn new(config: Config) -> Result<Self, BoxError> {
        config.validate()?;
        Ok(Self { config })
    }
}
#[async_trait]
impl ModelProvider for LocalProvider {
    async fn list_models(&self) -> Result<Vec<ModelEntry>, BoxError> {
        Ok(self
            .config
            .server
            .iter()
            .flat_map(|s| {
                s.profile.iter().map(move |p| ModelEntry {
                    model_id: format!("{}/{}", s.name, p.id),
                    display_name: p.display_name.clone(),
                    context_window: s.context_window,
                    max_output_tokens: p.max_output_tokens,
                    supports_image_input: s.supports_image_input,
                    keeps_unsigned_reasoning: true,
                })
            })
            .collect())
    }
    async fn invoke_model(
        &self,
        _: &str,
        _: CompletionRequest,
    ) -> Result<ModelStream, CompletionError> {
        Err(CompletionError::provider(
            ErrorClass::Fatal,
            "local request dispatch is not yet implemented",
        ))
    }
}

"""Chain for interacting with ElasticSearch."""
from __future__ import annotations

import warnings
from typing import Any, Dict, List, Optional

from pydantic import Extra, Field, root_validator

from langchain.base_language import BaseLanguageModel
from langchain.callbacks.manager import CallbackManagerForChainRun
from langchain.chains.base import Chain
from langchain.chains.llm import LLMChain
from langchain.chains.es_engine.prompt import DECIDER_PROMPT, ES_PROMPT
from langchain.prompts.base import BasePromptTemplate
from langchain.es_engine import ESEngine


class ESChain(Chain):
    """Chain for interacting with Elasticsearch.

    Example:
        .. code-block:: python

            from langchain import ESChain, OpenAI, ESEngine
            engine = ESEngine(...)
            es_chain = ESChain.from_llm(OpenAI(), engine)
    """

    llm_chain: LLMChain
    llm: Optional[BaseLanguageModel] = None
    """[Deprecated] LLM wrapper to use."""
    engine: ESEngine = Field(exclude=True)
    """Elasticsearch to connect to."""
    prompt: Optional[BasePromptTemplate] = None
    """[Deprecated] Prompt to use to translate natural language to DSL."""
    top_k: int = 5
    """Number of results to return from the query"""
    index_name_input_key: str = "index_name"  #: :meta private:
    query_input_key: str = "query"  #: :meta private:
    output_key: str = "result"  #: :meta private:
    return_intermediate_steps: bool = False
    """Whether or not to return the intermediate steps along with the final answer."""
    return_direct: bool = False
    """Whether or not to return the result of querying the index directly."""

    class Config:
        """Configuration for this pydantic object."""

        extra = Extra.forbid
        arbitrary_types_allowed = True

    @root_validator(pre=True)
    def raise_deprecation(cls, values: Dict) -> Dict:
        if "llm" in values:
            warnings.warn(
                "Directly instantiating an ESChain with an llm is deprecated. "
                "Please instantiate with llm_chain argument or using the from_llm "
                "class method."
            )
            if "llm_chain" not in values and values["llm"] is not None:
                engine = values["engine"]
                prompt = values.get("prompt") or ES_PROMPT
                values["llm_chain"] = LLMChain(llm=values["llm"], prompt=prompt)
        return values

    @property
    def input_keys(self) -> List[str]:
        """Return the singular input key.

        :meta private:
        """
        return [self.index_name_input_key, self.query_input_key]

    @property
    def output_keys(self) -> List[str]:
        """Return the singular output key.

        :meta private:
        """
        if not self.return_intermediate_steps:
            return [self.output_key]
        else:
            return [self.output_key, "intermediate_steps"]

    def _call(
        self,
        inputs: Dict[str, Any],
        run_manager: Optional[CallbackManagerForChainRun] = None,
    ) -> Dict[str, Any]:
        _run_manager = run_manager or CallbackManagerForChainRun.get_noop_manager()
        input_text = f"{inputs[self.input_key]}\nDSLQuery:"
        _run_manager.on_text(input_text, verbose=self.verbose)
        # If not present, then defaults to None which is all indices.
        index_names_to_use = inputs.get("index_names")
        index_info = self.engine.get_index_info(index_names=index_names_to_use)
        _run_manager.on_text(index_info, color="yellow", verbose=self.verbose)
        llm_inputs = {
            "input": input_text,
            "top_k": self.top_k,
            "index_info": index_info,
            "stop": ["\nDSLResult:"],
        }
        intermediate_steps = []
        dsl_cmd = self.llm_chain.predict(
            callbacks=_run_manager.get_child(), **llm_inputs
        )
        intermediate_steps.append(dsl_cmd)
        _run_manager.on_text(dsl_cmd, color="green", verbose=self.verbose)
        result = self.engine.run(dsl_cmd)
        intermediate_steps.append(result)
        _run_manager.on_text("\nDSLResult: ", verbose=self.verbose)
        _run_manager.on_text(result, color="yellow", verbose=self.verbose)
        # If return direct, we just set the final result equal to the DSL query
        if self.return_direct:
            final_result = result
        else:
            _run_manager.on_text("\nAnswer:", verbose=self.verbose)
            input_text += f"{dsl_cmd}\nDSLResult: {result}\nAnswer:"
            llm_inputs["input"] = input_text
            final_result = self.llm_chain.predict(
                callbacks=_run_manager.get_child(), **llm_inputs
            )
            _run_manager.on_text(final_result, color="green", verbose=self.verbose)
        chain_result: Dict[str, Any] = {self.output_key: final_result}
        if self.return_intermediate_steps:
            chain_result["intermediate_steps"] = intermediate_steps
        return chain_result

    @property
    def _chain_type(self) -> str:
        return "es_chain"

    @classmethod
    def from_llm(
        cls,
        llm: BaseLanguageModel,
        engine: ESEngine,
        prompt: Optional[BasePromptTemplate] = None,
        **kwargs: Any,
    ) -> ESChain:
        prompt = prompt or ES_PROMPT
        llm_chain = LLMChain(llm=llm, prompt=prompt)
        return cls(llm_chain=llm_chain, engine=engine, **kwargs)


class ESSequentialChain(Chain):
    """Chain for querying Elasticsearch that is a sequential chain.

    The chain is as follows:
    1. Based on the query, determine which indices to use.
    2. Based on those indices, call the normal ES chain.

    This is useful in cases where the number of indices in the ES is large.
    """

    decider_chain: LLMChain
    es_chain: ESChain
    query_input_key: str = "query"  #: :meta private:
    output_key: str = "result"  #: :meta private:
    return_intermediate_steps: bool = False

    @classmethod
    def from_llm(
        cls,
        llm: BaseLanguageModel,
        engine: ESEngine,
        query_prompt: BasePromptTemplate = ES_PROMPT,
        decider_prompt: BasePromptTemplate = DECIDER_PROMPT,
        **kwargs: Any,
    ) -> ESSequentialChain:
        """Load the necessary chains."""
        es_chain = ESChain(
            llm=llm, engine=engine, prompt=query_prompt, **kwargs
        )
        decider_chain = LLMChain(
            llm=llm, prompt=decider_prompt, output_key="index_names"
        )
        return cls(es_chain=es_chain, decider_chain=decider_chain, **kwargs)

    @property
    def input_keys(self) -> List[str]:
        """Return the singular input key.

        :meta private:
        """
        return [self.query_input_key]

    @property
    def output_keys(self) -> List[str]:
        """Return the singular output key.

        :meta private:
        """
        if not self.return_intermediate_steps:
            return [self.output_key]
        else:
            return [self.output_key, "intermediate_steps"]

    def _call(
        self,
        inputs: Dict[str, Any],
        run_manager: Optional[CallbackManagerForChainRun] = None,
    ) -> Dict[str, Any]:
        _run_manager = run_manager or CallbackManagerForChainRun.get_noop_manager()
        _index_names = self.es_chain.engine.get_usable_index_names()
        index_names = ", ".join(_index_names)
        llm_inputs = {
            "query": inputs[self.query_input_key],
            "index_names": index_names,
        }
        index_names_to_use = self.decider_chain.predict_and_parse(
            callbacks=_run_manager.get_child(), **llm_inputs
        )
        _run_manager.on_text("Index names to use:", end="\n", verbose=self.verbose)
        _run_manager.on_text(
            str(index_names_to_use), color="yellow", verbose=self.verbose
        )
        new_inputs = {
            self.es_chain.query_input_key: inputs[self.query_input_key],
            self.es_chain.index_name_input_key: index_names_to_use,
        }
        print(str(new_inputs))
        _run_manager.on_text(
            str(new_inputs), color="red", verbose=self.verbose
        )
        return self.es_chain(
            new_inputs, callbacks=_run_manager.get_child(), return_only_outputs=True
        )

    @property
    def _chain_type(self) -> str:
        return "es_sequential_chain"

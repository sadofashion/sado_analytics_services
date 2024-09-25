ifeq (${TARGET},prod)
	FLAGS=--target prod
endif

deploy:
	lightdash deploy --target prod --ignore-errors

preview:
	lightdash preview ${FLAGS}
	
ifdef MODELS
generate:
	@echo "Generating Lightdash model yamls..."
	@echo "MODELS: ${MODELS}"
	lightdash generate -s ${MODELS}
	exit 0
endif
ifndef MODELS
generate:
	@echo "Generating Lightdash model yamls..."
	lightdash generate
endif

run-generate:
	@echo "MODELS: ${MODELS}"
	@echo "Running models..."
	dbt run -s ${MODELS} ${FLAGS}
	@echo "Regenerate model yamls"
	lightdash generate -s ${MODELS} ${FLAGS}

run-generate-all:
	@echo "Running all models..."
	dbt run -s ${MODELS} ${FLAGS}
	@echo "Generating Lightdash all model yamls..."
	lightdash generate ${FLAGS}

val:
	@echo "Validating yamls..."
	lightdash validate

gh:
	git push origin head
	gh run watch && git pull origin master

run-modified:
	@echo "Running modified models..."
	dbt run --select state:modified --defer --state prod ${FLAGS}